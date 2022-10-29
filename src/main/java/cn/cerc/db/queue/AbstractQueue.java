package cn.cerc.db.queue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.queue.QueueConsumer.OnMessageCallback;

public abstract class AbstractQueue implements OnMessageCallback {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);
    private QueueConsumer consumer;
    private long delayTime = 0L;

    public AbstractQueue() {
        super();
        log.info("Queue {} {} is init ", this.getClass().getSimpleName(), getTopic());
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

    public void startPushService() {
        this.consumer = QueueConsumer.getConsumer(this.getTopic(), this.getTag());
    }

    public void close() {
        if (consumer != null) {
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

    public void receiveMessage(int maxMessageNum) throws ClientException, IOException {
        final ClientServiceProvider provider = QueueServer.loadService();
        ClientConfiguration clientConfiguration = QueueServer.getConfig();
        String consumerGroup = this.getGroupId();
        Duration awaitDuration = Duration.ofSeconds(30);
        FilterExpression filterExpression = new FilterExpression(this.getTag(), FilterExpressionType.TAG);
        SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                // Set the consumer group name.
                .setConsumerGroup(consumerGroup)
                // set await duration for long-polling.
                .setAwaitDuration(awaitDuration)
                // Set the subscription for the consumer.
                .setSubscriptionExpressions(Collections.singletonMap(this.getTopic(), filterExpression))
                .build();
        // Set message invisible duration after it is received.
        Duration invisibleDuration = Duration.ofSeconds(10);
        final List<MessageView> messages = consumer.receive(maxMessageNum, invisibleDuration);
        for (MessageView message : messages) {
            try {
                Charset charset = Charset.forName("utf-8");
                String data = charset.decode(message.getBody()).toString();
                System.out.println("收到一条消息：" + data);
                consumer.ack(message);
            } catch (Throwable t) {
                log.error("Failed to acknowledge message, messageId={}", message.getMessageId(), t);
            }
        }
        consumer.close();
    }

}
