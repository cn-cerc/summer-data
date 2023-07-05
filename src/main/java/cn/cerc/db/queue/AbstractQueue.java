package cn.cerc.db.queue;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.Datetime.DateType;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.queue.mns.MnsServer;
import cn.cerc.db.queue.rabbitmq.RabbitQueue;
import cn.cerc.db.queue.sqlmq.SqlmqQueue;
import cn.cerc.db.queue.sqlmq.SqlmqServer;
import cn.cerc.db.redis.Redis;
import cn.cerc.db.zk.ZkConfig;

public abstract class AbstractQueue implements OnStringMessage, Watcher, Runnable {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);

    // 创建一个缓存线程池，在必要的时候在创建线程，若线程空闲60秒则终止该线程
//    public static final ExecutorService pool = Executors.newCachedThreadPool();

    /**
     * 获取当前JVM运行环境可调用的处理器线程数
     */
    private static final int processors = Runtime.getRuntime().availableProcessors();
    /**
     * 核心的线程数 -> CPU 全核心 <br>
     * 最大的线程数 -> CPU 全核心 * 4 <br>
     * 线程存活时间 -> 60秒 <br>
     * 工作队列大小 -> 1024 <br>
     */
    public static final ThreadPoolExecutor executor = new ThreadPoolExecutor(processors, processors * 4, 60,
            TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024), new ThreadPoolExecutor.CallerRunsPolicy());

    private static ZkConfig config;
    private boolean pushMode = false; // 默认为拉模式
    private QueueServiceEnum service;
    private int delayTime = 60; // 失败重试时间 单位：秒
    private Optional<Datetime> showTime = Optional.empty(); // 队列延时时间 默认当前时间
    private String original;
    private String order;
    private String groupCode;
    private int priorId;

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
     * @param delayTime 设置失败重试时间，单位：秒
     */
    protected void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }

    public final int getDelayTime() {
        return this.delayTime;
    }

    /**
     * 不要在单例模式下使用该方法推送延时消息！单例模式下showTime可能会被其他线程更改！
     * 
     * @param showTime 设置延迟时间
     */
    protected void setShowTime(Datetime showTime) {
        this.showTime = Optional.ofNullable(showTime);
    }

    public final Optional<Datetime> getShowTime() {
        return this.showTime;
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
        case Redis -> {
            try (Redis redis = new Redis()) {
                redis.lpush(this.getId(), data);
                return "push redis ok";
            }
        }
        case AliyunMNS -> {
            return MnsServer.getQueue(this.getId()).push(data);
        }
        case Sqlmq -> {
            if (this.priorId > 0)
                this.setShowTime(new Datetime().inc(DateType.Year, 1));
            SqlmqQueue sqlQueue = SqlmqServer.getQueue(this.getId());
            sqlQueue.setDelayTime(delayTime);
            sqlQueue.setShowTime(showTime.orElseGet(Datetime::new));
            sqlQueue.setService(service);
            sqlQueue.setQueueClass(this.getClass().getSimpleName());
            return sqlQueue.push(data, this.order, this.groupCode, this.priorId);
        }
        case RabbitMQ -> {
            try (RabbitQueue queue = new RabbitQueue(this.getId())) {
                return queue.push(data);
            }
        }
        default -> {
            return null;
        }
        }
    }

    @Override
    public void run() {
        switch (getService()) {
        case Redis -> {
            try (Redis redis = new Redis()) {
                var data = redis.rpop(this.getId());
                if (data != null)
                    this.consume(data, true);
            }
        }
        case AliyunMNS -> MnsServer.getQueue(getId()).pop(100, this);
        case Sqlmq -> SqlmqServer.getQueue(getId()).pop(100, this);
        case RabbitMQ -> {
            try (var queue = new RabbitQueue(this.getId())) {
                queue.setMaximum(100);
                queue.pop(this);
            }
        }
        default -> log.error("{} 不支持消息拉取模式:", getService().name());
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
            case Redis, AliyunMNS, RabbitMQ -> {
                log.debug("thread pool add {} job {}", Thread.currentThread(), this.getClass().getSimpleName());
                executor.submit(this);// 使用线程池
            }
            case Sqlmq -> {
                log.debug("{} sqlmq add job {}", Thread.currentThread(), this.getClass().getSimpleName());
                this.run();
            }
            default -> {
            }
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

    public static void close() {
        executor.shutdown();
        try {
            boolean awaited = executor.awaitTermination(5, TimeUnit.MINUTES);
            if (!awaited)
                log.error("队列线程池等待关闭失败");
        } catch (InterruptedException e) {
            log.error("等待线程池关闭超时了 {}", e.getMessage(), e);
        }

        if (executor.isTerminated()) {
            // 所有任务已完成
            log.info("队列线程池中的任务已全部执行完毕");
        } else {
            // 仍有任务在执行
            log.warn("仍有线程任务执行 ->");
            log.warn("当前的核心线程数 {}", executor.getCorePoolSize());
            log.warn("当前的线程池大小 {}", executor.getPoolSize());
            log.warn("当前的活动线程数 {}", executor.getActiveCount());
            log.warn("等待执行的任务数 {}", executor.getQueue().size());
            log.warn("已完成的任务数量 {}", executor.getCompletedTaskCount());
            log.warn("累计的总任务数量 {}", executor.getTaskCount());
        }
    }

    public String getGroupCode() {
        return groupCode;
    }

    public void setGroupCode(String groupCode) {
        this.groupCode = groupCode;
    }

    public int getPriorId() {
        return priorId;
    }

    public void setPriorId(int priorId) {
        this.priorId = priorId;
    }

}
