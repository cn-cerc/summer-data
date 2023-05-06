package cn.cerc.db.queue;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.queue.mns.MnsServer;
import cn.cerc.db.queue.rabbitmq.RabbitQueue;
import cn.cerc.db.queue.sqlmq.SqlmqServer;
import cn.cerc.db.redis.Redis;
import cn.cerc.db.zk.ZkConfig;

public abstract class AbstractQueue implements OnStringMessage, Watcher, Runnable {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);

    // 创建一个缓存线程池，在必要的时候在创建线程，若线程空闲60秒则终止该线程
//    public static final ExecutorService pool = Executors.newCachedThreadPool();

    public static final ThreadPoolExecutor pool = new ThreadPoolExecutor(8, 16, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.AbortPolicy());

    private static ZkConfig config;
    private boolean pushMode = false; // 默认为拉模式
    private QueueServiceEnum service;
    private int delayTime = 60; // 单位：秒
    private String original;
    private String order;

    public AbstractQueue() {
        super();
        // 配置消息服务方式：redis/mns/rocketmq
        this.setService(ServerConfig.getQueueService());
        // 配置产业代码：csp/fpl/obm/oem/odm
        this.setOriginal(ServerConfig.getAppOriginal());
    }

    public String getTopic() {
        return this.getClass().getSimpleName();
    }

    public final String getTag() {
        return String.format("%s-%s", ServerConfig.getAppVersion(), getOriginal());
    }

    public final String getId() {
        Objects.requireNonNull(this.getTopic());
        return this.getTopic() + "-" + getTag();
    }

    public String getOriginal() {
        return original;
    }

    /**
     * 切换消息队列所指向的机群，如FPL/OBM/CSM等
     * 
     * @param original
     */
    protected void setOriginal(String original) {
        Objects.requireNonNull(original);
        this.original = original;
    }

    /**
     * 
     * @param delayTime 设置延迟时间，单位：秒
     */
    protected void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }

    // 创建延迟队列消息
    public final long getDelayTime() {
        return this.delayTime;
    }

    public void startService() {
        // 通知ZooKeeper
        try {
            ZkConfig host = new ZkConfig(String.format("/app/%s", ServerConfig.getAppName()));
            String child = host.path("status");
            var stat = host.client().exists(child, this);
            if (stat == null) {
                host.setValue("status", "running");
                stat = host.client().exists(child, this);
                if (stat == null) {
                    log.warn("配置有误，无法启动消息队列");
                    return;
                }
            }
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            return;
        }
        config().setTempNode(this.getClass().getSimpleName(), "running");
        log.info("注册消息服务：{} from {}", this.getId(), this.getService().name());
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Watcher.Event.EventType.DataWatchRemoved) {
            log.info("此主机运行状态被移除");
            this.stopService();
        }
    }

    public void stopService() {
        log.info("{} 关闭了消息推送服务", this.getTopic());
        config().delete(this.getClass().getSimpleName());
    }

    private ZkConfig config() {
        if (config == null)
            config = new ZkConfig(String.format("/app/%s/task", ServerConfig.getAppName()));
        return config;
    }

    protected String push(String data) {
        switch (getService()) {
        case Redis:
            try (Redis redis = new Redis()) {
                redis.lpush(this.getId(), data);
                return "push redis ok";
            }
        case AliyunMNS:
            return MnsServer.getQueue(this.getId()).push(data);
        case Sqlmq:
            var sqlQueue = SqlmqServer.getQueue(this.getId());
            sqlQueue.setDelayTime(delayTime);
            sqlQueue.setService(service);
            sqlQueue.setQueueClass(this.getClass().getSimpleName());
            return sqlQueue.push(data, this.order);
        case RabbitMQ: {
            try (var queue = new RabbitQueue(this.getId())) {
                return queue.push(data);
            }
        }
        default:
            return null;
        }
    }

    @Override
    public void run() {
        switch (getService()) {
        case Redis:
            try (Redis redis = new Redis()) {
                var data = redis.rpop(this.getId());
                if (data != null)
                    this.consume(data, true);
            }
            break;
        case AliyunMNS:
            MnsServer.getQueue(getId()).pop(100, this);
            break;
        case Sqlmq:
            SqlmqServer.getQueue(getId()).pop(100, this);
            break;
        case RabbitMQ: {
            try (var queue = new RabbitQueue(this.getId())) {
                queue.setMaximum(100);
                queue.pop(this);
            }
        }
            break;
        default:
            log.error("{} 不支持消息拉取模式:", getService().name());
        }
    }

    public final QueueServiceEnum getService() {
        return service;
    }

    public final void setService(QueueServiceEnum service) {
        this.service = service;
    }

    /**
     * 默认3秒检测一次，若某个消息耗时过高，可将此函数覆盖为空函数
     */
    @Scheduled(initialDelay = 30000, fixedRate = 3000)
    public void defaultCheck() {
        if (this.isPushMode())
            return;

        if (ServerConfig.enableTaskService()) {
            switch (this.getService()) {
            case Redis, AliyunMNS, RabbitMQ:
                log.debug("thread pool add {} job {}", Thread.currentThread(), this.getClass().getSimpleName());
                pool.submit(this);// 使用线程池
                break;
            case Sqlmq:
                log.debug("{} sqlmq add job {}", Thread.currentThread(), this.getClass().getSimpleName());
                this.run();
                break;
            default:
                break;
            }
        }
    }

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }

    /**
     * 
     * @return 为true表示为推模式
     */
    public boolean isPushMode() {
        return pushMode;
    }

    protected void setPushMode(boolean pushMode) {
        this.pushMode = pushMode;
    }

    protected void pushToSqlmq(String message) {
        if (this.getService() == QueueServiceEnum.Sqlmq)
            return;
        var queue = SqlmqServer.getQueue(this.getId());
        queue.setService(this.service);
        queue.setDelayTime(this.delayTime);
        queue.setQueueClass(this.getClass().getSimpleName());
        queue.push(message, this.order);
    }

}
