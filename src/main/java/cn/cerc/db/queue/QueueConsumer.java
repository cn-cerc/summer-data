package cn.cerc.db.queue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
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
import cn.cerc.db.core.ServerConfig;

public class QueueConsumer {
    private static final Logger log = LoggerFactory.getLogger(DataSet.class);
    protected static final Map<String, QueueConsumer> consumers = new HashMap<>();

    private String topic;
    private String tag;
    private PushConsumer consumer;

    public interface OnMessageCallback {
        boolean consume(String message);
    }

    public static QueueConsumer create(String topic, String tag, OnMessageCallback callback) {
        if (consumers.containsKey(String.format("%s-%s", topic, tag)))
            return consumers.get(String.format("%s-%s", topic, tag));
        QueueConsumer consumer2 = new QueueConsumer(topic, tag, callback);
        consumers.put(String.format("%s-%s", topic, tag), consumer2);
        return consumer2;
    }

    public PushConsumer consumer() {
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

    private QueueConsumer(String topic, String tag, OnMessageCallback callback) {
        this.topic = topic;
        this.tag = tag;

        if (!ServerConfig.enableTaskService())
            return;
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

        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        try {
            PushConsumer consumer = provider.newPushConsumerBuilder()
                    .setClientConfiguration(clientConfiguration)
                    .setConsumerGroup(consumerGroup)
                    .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                    .setMessageListener(
                            message -> callback.consume(StandardCharsets.UTF_8.decode(message.getBody()).toString())
                                    ? ConsumeResult.SUCCESS
                                    : ConsumeResult.FAILURE)
                    .build();
            this.consumer = consumer;
        } catch (Exception e) {
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
//        try {
//            List<MessageView> messages = consumer.receive(1, Duration.ofMinutes(10));
//            for (MessageView message : messages) {
//                return message;
//            }
//        } catch (ClientException e) {
//            log.error(e.getMessage(), e);
//        }
//        return null;
        return null;
    }

    /**
     * 删除消息
     */
    public void delete(MessageView message) {
    }

    public static void main(String[] args) throws Exception {
        List<String> tags = new ArrayList<>();
        tags.add("a");
        tags.add("b");
        tags.add("c");
        tags.add("d");
        tags.add("e");
        tags.add("f");
        tags.add("g");
        tags.add("h");
        tags.add("i");
        tags.add("j");
        tags.add("k");
        tags.add("l");
        tags.add("m");
        QueueServer.createTopic("test",false);

        tags.forEach(tag -> {
            new Thread(() -> {
                create("test", tag, message -> {
                    System.out.println(tag + "---" + message);
                    return true;
                });
            }).start();
        });

//        Client client = QueueServer.getClient();
//        ListTopicsRequest request = new ListTopicsRequest();
//        request.setPageNumber(1);
//        request.setPageSize(100);
//        ListTopicsResponse response = client.listTopics(QueueServer.getInstanceId(), request);
//        response.getBody().getData().getList().forEach(item -> {
//            try {
//                client.deleteTopic(QueueServer.getInstanceId(), item.getTopicName());
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });

//        Client client = QueueServer.getClient();
//        ListConsumerGroupsRequest request = new ListConsumerGroupsRequest();
//        request.setPageNumber(1);
//        request.setPageSize(100);
//        var response = client.listConsumerGroups(QueueServer.getInstanceId(), request);
//        response.getBody().getData().getList().forEach(item -> {
//            try {
//                client.deleteConsumerGroup(QueueServer.getInstanceId(), item.getConsumerGroupId());
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
    }

}
