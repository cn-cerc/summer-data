package cn.cerc.db.queue;

import java.time.Duration;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.redis.Redis;
import cn.cerc.db.zk.ZkConfig;

public abstract class AbstractQueue implements OnStringMessage, Watcher {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);
    private static QueueConsumer consumer;
    private static ZkConfig config;
    private QueueServiceEnum service;
    private long delayTime = 0L;

    public AbstractQueue() {
        super();
        this.setService(ServerConfig.getQueueService());
        // 检查消费主题、队列组是否有创建
        switch (service) {
        case Redis:
            break;
        case RocketMQ:
            QueueServer.createTopic(this.getTopic(), this.getDelayTime() > 0);
            break;
        default:
            throw new RuntimeException("不支持的消息设备：" + service.name());
        }
    }

    public abstract String getTopic();

    public String getTag() {
        return QueueConfig.tag;
    }

    // 创建延迟队列消息
    public long getDelayTime() {
        return this.delayTime;
    }

    public void startService(QueueConsumer consumer) {
        AbstractQueue.consumer = consumer;
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

        log.info("注册消息推送服务：{}", this.getTopic());
        if (this.service == QueueServiceEnum.RocketMQ)
            consumer.addConsumer(this.getTopic(), this.getTag(), this);
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
        if (this.service == QueueServiceEnum.RocketMQ) {
            synchronized (AbstractQueue.class) {
                if (consumer != null) {
                    consumer.close();
                    consumer = null;
                }
            }
        }
    }

    private ZkConfig config() {
        if (config == null)
            config = new ZkConfig(String.format("/app/%s/task", ServerConfig.getAppName()));
        return config;
    }

    private String getId() {
        return this.getTopic() + "-" + getTag();
    }

    protected String sendMessage(String data) {
        switch (service) {
        case Redis:
            try (Redis redis = new Redis()) {
                redis.lpush(this.getId(), data);
                return "push redis ok";
            }
        case RocketMQ:
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
        default:
            return null;
        }
    }

    protected void receiveMessage() {
        switch (service) {
        case RocketMQ:
            log.error("RocketMQ 不支持 receiveMessage");
            break;
        default:
            try (Redis redis = new Redis()) {
                var data = redis.rpop(this.getId());
                if (data != null)
                    this.consume(data);
            }
        }
    }

    protected QueueServiceEnum getService() {
        return service;
    }

    public void setService(QueueServiceEnum service) {
        this.service = service;
    }

    /**
     * 默认3秒检测一次，若某个消息耗时过高，可将此函数覆盖为空函数
     */
    @Scheduled(initialDelay = 30000, fixedRate = 3000)
    public void defaultCheck() {
        if (service == QueueServiceEnum.Redis && ServerConfig.enableTaskService())
            this.receiveMessage();
    }
}
