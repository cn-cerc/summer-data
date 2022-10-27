package cn.cerc.db.queue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.rocketmq20220801.Client;
import com.aliyun.rocketmq20220801.models.CreateConsumerGroupRequest;
import com.aliyun.rocketmq20220801.models.CreateConsumerGroupRequest.CreateConsumerGroupRequestConsumeRetryPolicy;
import com.aliyun.rocketmq20220801.models.CreateConsumerGroupResponse;
import com.aliyun.rocketmq20220801.models.GetConsumerGroupResponse;
import com.aliyun.rocketmq20220801.models.GetConsumerGroupResponseBody.GetConsumerGroupResponseBodyData;

import cn.cerc.db.core.DataSet;

public class QueueConsumer {
    private static final Logger log = LoggerFactory.getLogger(DataSet.class);
    private static final ClientServiceProvider provider = ClientServiceProvider.loadService();
    protected static final Map<String, SimpleConsumer> consumers = new HashMap<>();
    private String topic;
    private String tag;
    private SimpleConsumer consumer;

    public static QueueConsumer create(String topic, String tag) {
        return new QueueConsumer(topic, tag);
    }

    public SimpleConsumer consumer() {
        return consumer;
    }

    public void close() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private QueueConsumer(String topic, String tag) {
        this.topic = topic;
        this.tag = tag;

        if (consumers.containsKey(topic)) {
            this.consumer = consumers.get(topic);
            return;
        }

        String consumerGroup = String.format("%s-%s-%s", "G", topic, tag);
        Client client = QueueServer.getClient();
        try {
            GetConsumerGroupResponse response = client.getConsumerGroup(QueueServer.getInstanceId(), consumerGroup);
            GetConsumerGroupResponseBodyData data = response.getBody().getData();
            if (data == null || !"RUNNING".equals(data.getStatus())) {
                CreateConsumerGroupRequest request = new CreateConsumerGroupRequest();
                request.setDeliveryOrderType("Concurrently");
                CreateConsumerGroupRequestConsumeRetryPolicy policy = new CreateConsumerGroupRequestConsumeRetryPolicy();
                policy.setMaxRetryTimes(16);
                policy.setRetryPolicy("FixedRetryPolicy");
                request.setConsumeRetryPolicy(policy);
                CreateConsumerGroupResponse createResponse = client.createConsumerGroup(QueueServer.getInstanceId(),
                        consumerGroup, request);
                if (!createResponse.getBody().getSuccess()) {
                    log.error("创建消费组 {} 失败");
                    return;
                }
            }
        } catch (Exception e1) {
            log.error(e1.getMessage(), e1);
            return;
        }

        log.info("{}, {}, {} ,{} consumer is creating", topic, tag, consumerGroup, Thread.currentThread());
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
            consumers.put(topic, this.consumer);
        } catch (ClientException e) {
            log.error(e.getMessage(), e);
        }
    }

    public String getTopic() {
        return topic;
    }

    public String getTag() {
        return tag;
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
            log.error(e.getMessage(), e);
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
            log.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args) {
        QueueConsumer create = create("TopicTestMQ", "fpl");
        MessageView receive = create.recevie();
        while (receive != null) {
            System.out.println(StandardCharsets.UTF_8.decode(receive.getBody()).toString());
            create.delete(receive);
            receive = create.recevie();
        }
        create.close();
    }

}
