package cn.cerc.db.queue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
    private String tag = "*";
    SimpleConsumer consumer;

    public QueueConsumer(String topic, String tag) throws ClientException {
        super();
        // Credential provider is optional for client configuration.
        String accessKey = QueueServer.getRmqAccessKeyId();
        String secretKey = QueueServer.getRmqAccessSecret();
        SessionCredentialsProvider sessionCredentialsProvider = new StaticSessionCredentialsProvider(accessKey,
                secretKey);
        String endpoints = QueueServer.getRmqEndpoint();
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(endpoints)
                .setCredentialProvider(sessionCredentialsProvider)
                .build();
        String consumerGroup = "main";
        Duration awaitDuration = Duration.ofSeconds(1);
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setConsumerGroup(consumerGroup)
                .setAwaitDuration(awaitDuration)
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                .build();
        this.consumer = consumer;
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
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 读取消息
     * 
     * @param processer
     * @return 返回读取的笔数
     * @throws ClientException
     * @throws MQClientException
     */
    public int recevie(QueueProcesser processer) throws ClientException {

        final List<MessageView> messages = consumer.receive(16, Duration.ofMinutes(10));
        for (MessageView message : messages) {
            try {
                if (processer.processMessage(StandardCharsets.UTF_8.decode(message.getBody()).toString()))
                    consumer.ack(message);
            } catch (Exception e) {
            }
        }
        return messages.size();
    }

    /**
     * 读取消息
     * 
     * @param processer
     * @return 返回读取的笔数
     * @throws ClientException
     * @throws MQClientException
     */
    public MessageView recevie() throws ClientException {

        final List<MessageView> messages = consumer.receive(1, Duration.ofMinutes(10));
        for (MessageView message : messages) {
            return message;
        }
        return null;
    }

    public void ack(MessageView msg) throws ClientException {
        consumer.ack(msg);
    }

    public static void main(String[] args) throws ClientException {

        QueueProcesser processer = data -> {
            System.out.println("消息内容: " + data);
            return true;
        };
        try (var consumer1 = new QueueConsumer("TopicTestMQ", "fpl")) {
            var count = consumer1.recevie(processer);
            System.out.println(String.format("有读到 %s 条消息", count));
        }
    }
}
