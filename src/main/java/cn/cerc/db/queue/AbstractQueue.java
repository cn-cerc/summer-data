package cn.cerc.db.queue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.queue.QueueConsumer.OnMessageCallback;
import cn.cerc.db.queue.QueueConsumer.OnPullQueue;

public abstract class AbstractQueue implements OnMessageCallback, ServletContextListener {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);
    private QueueConsumer consumer;
    private long delayTime = 0L;

    public AbstractQueue() {
        super();
        log.info("{} {} is init ", this.getClass().getSimpleName(), getTopic());
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
        if (consumer == null) {
            log.info("{} 启动了消息推送服务", this.getTopic());
            consumer = QueueConsumer.getConsumer(this.getTopic(), this.getTag());
            consumer.createQueueGroup(this);
        }
    }

    public void stopService() {
        if (consumer != null) {
            log.info("{} 关闭了消息推送服务", this.getTopic());
            consumer.close();
            consumer = null;
        }
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

    public synchronized void pullMessage(OnPullQueue pull, int maxMessageNum) throws ClientException, IOException {
        String consumerGroup = this.getGroupId();
        // 拉取时，等服务器多久
        Duration awaitDuration = Duration.ofSeconds(0L);
        ClientConfiguration clientConfiguration = QueueServer.getConfig();
        FilterExpression filterExpression = new FilterExpression(this.getTag(), FilterExpressionType.TAG);
        final ClientServiceProvider provider = QueueServer.loadService();
        SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setConsumerGroup(consumerGroup)
                // set await duration for long-polling.
                .setAwaitDuration(awaitDuration)
                // Set the subscription for the consumer.
                .setSubscriptionExpressions(Collections.singletonMap(this.getTopic(), filterExpression))
                .build();
        // Set message invisible duration after it is received.
        Duration invisibleDuration = Duration.ofSeconds(10);
        final List<MessageView> messages = consumer.receive(maxMessageNum, invisibleDuration);
        try {
            if (messages.size() > 0) {
                pull.startPull();
                for (MessageView message : messages) {
                    try {
                        Charset charset = Charset.forName("utf-8");
                        String data = charset.decode(message.getBody()).toString();
                        System.out.println("收到一条消息：" + data);
                        if (pull.consume(data))
                            consumer.ack(message);
                    } catch (Throwable t) {
                        log.error("Failed to acknowledge message, messageId={}", message.getMessageId(), t);
                    }
                }
                pull.stopPull();
            }
        } finally {
            consumer.close();
        }
    }

}
