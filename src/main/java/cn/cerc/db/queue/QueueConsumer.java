package cn.cerc.db.queue;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;

public class QueueConsumer implements AutoCloseable {
    private static final ClientServiceProvider provider = ClientServiceProvider.loadService();
    private String topic;
    private String tag;
    private SimpleConsumer consumer;
    private static final String consumerGroup = "main";// 默认分组,强制要求

    public static QueueConsumer create(String topic, String tag) {
        return new QueueConsumer(topic, tag);
    }

    public QueueConsumer(String topic, String tag) {
        String accessKey = QueueServer.getAccessKeyId();
        String secretKey = QueueServer.getAccessSecret();
        SessionCredentialsProvider sessionCredentialsProvider = new StaticSessionCredentialsProvider(accessKey,
                secretKey);

        String endpoints = QueueServer.getEndpoint();
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(endpoints)
                .setCredentialProvider(sessionCredentialsProvider)
                .build();

        Duration awaitDuration = Duration.ofSeconds(1);
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        SimpleConsumer consumer;
        try {
            consumer = provider.newSimpleConsumerBuilder()
                    .setClientConfiguration(clientConfiguration)
                    .setConsumerGroup(consumerGroup)
                    .setAwaitDuration(awaitDuration)
                    .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                    .build();
            this.consumer = consumer;
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

    public String getTopic() {
        return topic;
    }

    public String getTag() {
        return tag;
    }

    @Override
    public void close() {
        try {
            if (consumer != null)
                consumer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取消息
     */
    public MessageView recevie() {
        try {
            List<MessageView> messages = consumer.receive(1, Duration.ofMinutes(10));
            for (MessageView message : messages) {
                return message;
            }
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除消息
     */
    public void delete(MessageView message) {
        if (message == null)
            return;
        try {
            consumer.ack(message);
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

}
