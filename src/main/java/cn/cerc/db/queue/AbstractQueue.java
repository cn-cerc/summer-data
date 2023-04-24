package cn.cerc.db.queue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.queue.mns.MnsServer;
import cn.cerc.db.queue.rabbitmq.RabbitQueue;
import cn.cerc.db.queue.sqlmq.SqlmqServer;
import cn.cerc.db.redis.Redis;
import cn.cerc.db.zk.ZkConfig;
import cn.cerc.db.zk.ZkServer;

public abstract class AbstractQueue implements OnStringMessage, Watcher, Runnable {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);

    // 创建一个缓存线程池，在必要的时候在创建线程，若线程空闲60秒则终止该线程
    public static final ExecutorService pool = Executors.newCachedThreadPool();

    private static final ZkConfig config = new ZkConfig("/queues");
    private static String hostName;

    private boolean pushMode = false; // 默认为拉模式
    /** 是否为单机运行队列 */
    private boolean singleRun = true;// 默认为单机运行队列
    private QueueServiceEnum service;
    private int delayTime = 60; // 单位：秒
    private String original;
    private String order;

    static {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            hostName = addr.getHostName() + ":" + Utils.newGuid();
        } catch (UnknownHostException e) {
            log.error(e.getMessage(), e);
            hostName = Utils.newGuid();
        }
    }

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

    public final void registerQueue() {
        String queueId = this.getId();
        String path = config.path(queueId);
        if (config.exists(queueId)) {
            ZkServer.get().watch(path, this);
            return;
        }
        // 通知ZooKeeper
        ZkServer.get().asyncSetValue(path, hostName, CreateMode.EPHEMERAL, (status, nodePath, ctx, name) -> {
            if (status == KeeperException.Code.OK.intValue()) {
                log.info("消息服务注册成功 {}", queueId);
                if (this.isPushMode() && this.getService() == QueueServiceEnum.RabbitMQ) {
                    new RabbitQueue(queueId).watch(this);
                }
            }
        }).watch(path, this);
    }

    @Override
    public void process(WatchedEvent event) {
        String queueId = this.getId();
        String path = config.path(queueId);
        if (event.getPath().equals(path)) {
            if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                if (isSingleRun()) {
                    log.info("重新注册消息服务 {}", queueId);
                    registerQueue();
                }
            }
        }
    }

    public final boolean allowRun() {
        if (!isSingleRun())
            return true;
        String queueId = this.getId();
        if (config.exists(queueId)) {
            return config.getString(queueId, "").equals(hostName);
        }
        return false;
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

        if (ServerConfig.enableTaskService() && this.allowRun()) {
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

    public boolean isSingleRun() {
        return singleRun;
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
