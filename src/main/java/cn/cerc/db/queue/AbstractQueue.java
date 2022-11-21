package cn.cerc.db.queue;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.queue.rabbitmq.RabbitServer;
import cn.cerc.db.queue.sqlmq.SqlmqServer;
import cn.cerc.db.redis.Redis;
import cn.cerc.db.zk.ZkConfig;

public abstract class AbstractQueue implements OnStringMessage, Watcher, Runnable {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);

    private static final int MAX_THREAD_SIZE = Runtime.getRuntime().availableProcessors();
    public static final ExecutorService pool = Executors.newFixedThreadPool(MAX_THREAD_SIZE);
    public static final Map<String, Future<?>> items = new ConcurrentHashMap<>();// 正在执行任务的线程列表

    private static ZkConfig config;
    private boolean pushMode = false; // 默认为拉模式
    private QueueServiceEnum service;
    private boolean initTopic;
    private long delayTime = 0L;
    private String industry;
    private String order;

    public AbstractQueue() {
        super();
        // 配置消息服务方式：redis/mns/rocketmq
        this.setService(ServerConfig.getQueueService());
        // 配置产业代码：csp/fpl/obm/oem/odm
        this.setIndustry(ServerConfig.getAppIndustry());
    }

    public String getTopic() {
        return this.getClass().getSimpleName();
    }

    public final String getTag() {
        return String.format("%s-%s", ServerConfig.getAppVersion(), getIndustry());
    }

    public final String getId() {
        return this.getTopic() + "-" + getTag();
    }

    public String getIndustry() {
        return industry;
    }

    public void setIndustry(String industry) {
        this.industry = industry;
    }

    protected void setIndustryByCorpNo(IHandle handle, String corpNo) {
        throw new RuntimeException("从数据库取得相应的产业代码");
    }

    protected void setDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }

    // 创建延迟队列消息
    public final long getDelayTime() {
        return this.delayTime;
    }

    public void startService(QueueConsumer consumer) {
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
        if (this.getService() == QueueServiceEnum.RocketMQ) {
            initTopic();
            consumer.addConsumer(this.getTopic(), this.getTag(), this);
        }
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
        case RocketMQ:
            this.initTopic();
            try {
                var producer = new QueueProducer(getTopic(), getTag());
                var messageId = producer.append(data, Duration.ofSeconds(getDelayTime()));
                log.info("发送消息成功  {} {} {}", getTopic(), getTag(), messageId);
                return messageId;
            } catch (ClientException e) {
                log.error(e.getMessage());
                e.printStackTrace();
                return null;
            }
        case Sqlmq:
            return SqlmqServer.getQueue(this.getId()).push(data, this.order);
        case RabbitMQ:
            return RabbitServer.getQueue(this.getId()).push(data);
        default:
            return null;
        }
    }

    private void initTopic() {
        if (this.initTopic)
            return;
        QueueServer.createTopic(this.getTopic(), this.getDelayTime() > 0);
        this.initTopic = true;
    }

    @Override
    public void run() {
        switch (getService()) {
        case Redis:
            try (Redis redis = new Redis()) {
                var data = redis.rpop(this.getId());
                if (data != null)
                    this.consume(data);
            }
            break;
        case AliyunMNS:
            MnsServer.getQueue(getId()).pop(100, this);
            break;
        case Sqlmq:
            SqlmqServer.getQueue(getId()).pop(100, this);
            break;
        case RabbitMQ:
            RabbitServer.getQueue(this.getId()).pop(100, this);
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
        int before = items.size();
        items.values().removeIf(Future::isDone);
        if (items.size() < before)
            log.info("清理之前 {} 清理之后 {}", before, items.size());

        if (this.isPushMode())
            return;

        if (ServerConfig.enableTaskService()) {
            switch (this.getService()) {
            case Redis, AliyunMNS, Sqlmq, RabbitMQ:
//                this.run();
//                new Thread(this).start(); // 直接开线程
                if (items.containsKey(this.getClass().getSimpleName())) {
                    log.warn("{} 当前类有任务正在执行中，等候下一轮排队吧", this.getClass().getSimpleName());
                    break;
                }

                Future<?> future = pool.submit(this);// 使用线程池
                items.put(this.getClass().getSimpleName(), future);
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

}
