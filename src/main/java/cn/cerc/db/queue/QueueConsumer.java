package cn.cerc.db.queue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.PushConsumerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.rocketmq20220801.Client;
import com.aliyun.rocketmq20220801.models.CreateConsumerGroupRequest;
import com.aliyun.rocketmq20220801.models.CreateConsumerGroupRequest.CreateConsumerGroupRequestConsumeRetryPolicy;
import com.aliyun.rocketmq20220801.models.CreateConsumerGroupResponse;
import com.aliyun.rocketmq20220801.models.GetConsumerGroupResponse;
import com.aliyun.rocketmq20220801.models.GetConsumerGroupResponseBody.GetConsumerGroupResponseBodyData;

import cn.cerc.db.core.DataSet;

public class QueueConsumer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DataSet.class);
    protected static final Map<String, QueueConsumer> consumers = new HashMap<>();
    private String topic;
    private String tag;
    private PushConsumer consumer;
    private ClientServiceProvider provider;

    public interface OnMessageCallback {
        boolean consume(String message);
    }

    public interface OnPullQueue extends OnMessageCallback {
        void startPull();
        void stopPull();
    }

    public static synchronized QueueConsumer getConsumer(String topic, String tag) {
        String key = String.format("%s-%s", topic, tag);
        if (consumers.containsKey(key))
            return consumers.get(key);
        QueueConsumer consumer = new QueueConsumer(topic, tag);
        consumers.put(key, consumer);
        return consumer;
    }

    public QueueConsumer(String topic, String tag) {
        super();
        this.topic = topic;
        this.tag = tag;
        this.provider = QueueServer.loadService();
    }

    @Override
    public void close() {
        if (consumer != null) {
            try {
                consumer.close();
                consumer = null;
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void createQueueTopic(boolean isDelayQueue) {
        QueueServer.createTopic(this.getTopic(), isDelayQueue);
    }

    public void createQueueGroup(OnMessageCallback watcher) {
        String groupId = this.getGroupId();
        Client client = QueueServer.getClient();
        try {
            // 查找指定的主题组是否存在
            GetConsumerGroupResponse response = client.getConsumerGroup(QueueServer.getInstanceId(), groupId);
            GetConsumerGroupResponseBodyData data = response.getBody().getData();
            if (data == null || !"RUNNING".equals(data.getStatus())) {
                // 创建主题组
                CreateConsumerGroupRequest request = new CreateConsumerGroupRequest();
                request.setDeliveryOrderType("Concurrently");
                CreateConsumerGroupRequestConsumeRetryPolicy policy = new CreateConsumerGroupRequestConsumeRetryPolicy();
                policy.setMaxRetryTimes(16);
                policy.setRetryPolicy("FixedRetryPolicy");
                request.setConsumeRetryPolicy(policy);
                CreateConsumerGroupResponse createResponse = client.createConsumerGroup(QueueServer.getInstanceId(),
                        groupId, request);
                if (!createResponse.getBody().getSuccess()) {
                    log.error("创建消费组 {} 失败");
                    return;
                }
            }
        } catch (Exception e1) {
            log.error(e1.getMessage(), e1);
            return;
        }

        if (watcher != null) {
            log.info("{}, {}, {} ,{} consumer is creating", topic, tag, groupId, Thread.currentThread());
            ClientConfiguration clientConfiguration = QueueServer.getConfig();
            FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
            try {
                PushConsumerBuilder builder = provider.newPushConsumerBuilder()
                        .setClientConfiguration(clientConfiguration)
                        .setConsumerGroup(groupId)
                        .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression));
                builder.setMessageListener(
                        message -> watcher.consume(StandardCharsets.UTF_8.decode(message.getBody()).toString())
                                ? ConsumeResult.SUCCESS
                                : ConsumeResult.FAILURE);
                this.consumer = builder.build();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public String getGroupId() {
        if (tag == null)
            return String.format("G-%s", topic);
        else
            return String.format("G-%s-%s", topic, tag);
    }

    public String getTopic() {
        return topic;
    }

    public String getTag() {
        return tag;
    }

}
