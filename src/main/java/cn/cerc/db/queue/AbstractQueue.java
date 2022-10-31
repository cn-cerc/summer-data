package cn.cerc.db.queue;

import java.time.Duration;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.rocketmq.client.apis.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.zk.ZkConfig;

public abstract class AbstractQueue implements OnStringMessage, ServletContextListener {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);
    private static ZkConfig config;
    private QueueConsumer consumer;
    private long delayTime = 0L;

    public AbstractQueue() {
        super();
        log.debug("{} {} is init ", this.getClass().getSimpleName(), getTopic());
        // 检查消费主题、队列组是否有创建
        try (QueueConsumer temp = new QueueConsumer(this.getTopic(), getTag())) {
            temp.createQueueTopic(this.getDelayTime() > 0);
            temp.createQueueGroup(null);
            log.debug("{}: 自动检查是否有创建此队列", this.getGroupId());
        }
    }

    public abstract String getTopic();

    public String getTag() {
        return QueueConfig.tag;
    }

    protected String getGroupId() {
        try (var consumer = new QueueConsumer(this.getTopic(), this.getTag())) {
            return consumer.getGroupId();
        }
    }

    // 创建延迟队列消息
    public long getDelayTime() {
        return this.delayTime;
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        if (ServerConfig.enableTaskService()) {
            this.startService();
        } else {
            log.info("当前主机有关闭消息队列服务");
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        this.stopService();
    }

    public void startService() {
        if (consumer != null)
            return;

        if (config == null)
            config = new ZkConfig(String.format("/app/%s/task", ServerConfig.getAppName()));
        config.setTempNode(this.getGroupId(), "running");

        log.info("{} 启动了消息推送服务", this.getTopic());
        consumer = QueueConsumer.getConsumer(this.getTopic(), this.getTag());
        consumer.createQueueGroup(this);
    }

    public void stopService() {
        if (consumer == null)
            return;

        if (config == null)
            config = new ZkConfig(String.format("/app/%s/task", ServerConfig.getAppName()));
        config.delete(this.getGroupId());

        log.info("{} 关闭了消息推送服务", this.getTopic());
        consumer.close();
        consumer = null;
    }

    protected String sendMessage(String data) {
        var producer = new QueueProducer(getTopic(), getTag());
        try {
            var messageId = producer.append(data, Duration.ofSeconds(this.delayTime));
            return messageId;
        } catch (ClientException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            return null;
        } finally {
            producer.close();
        }
    }
}
