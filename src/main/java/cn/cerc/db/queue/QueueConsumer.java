package cn.cerc.db.queue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.PushConsumerBuilder;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import com.aliyun.rocketmq20220801.Client;
import com.aliyun.rocketmq20220801.models.CreateConsumerGroupRequest;
import com.aliyun.rocketmq20220801.models.CreateConsumerGroupRequest.CreateConsumerGroupRequestConsumeRetryPolicy;
import com.aliyun.rocketmq20220801.models.CreateConsumerGroupResponse;
import com.aliyun.rocketmq20220801.models.GetConsumerGroupResponse;
import com.aliyun.rocketmq20220801.models.GetConsumerGroupResponseBody.GetConsumerGroupResponseBodyData;

import cn.cerc.db.core.ServerConfig;

@Component
public class QueueConsumer implements AutoCloseable, ApplicationListener<ApplicationContextEvent>, OnMessageRecevie {
    private static final Logger log = LoggerFactory.getLogger(QueueConsumer.class);
    private static final Map<String, OnStringMessage> items1 = new HashMap<>();
    private static final Map<String, FilterExpression> items2 = new HashMap<>();
    private PushConsumer pushConsumer;
    private SimpleConsumer pullConsumer;

    public QueueConsumer() {
        super();
    }

    @Override
    public void close() {
        if (pushConsumer != null) {
            try {
                pushConsumer.close();
                pushConsumer = null;
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
        if (pullConsumer != null) {
            try {
                pullConsumer.close();
                pullConsumer = null;
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void addConsumer(String topic, String tag, OnStringMessage event) {
        String key = topic + "-" + tag;
        log.debug("register consumer: {}", key);
        items1.put(key, event);
        items2.put(topic, new FilterExpression(tag, FilterExpressionType.TAG));
    }

    @Override
    public boolean consume(MessageView message) {
        String key = message.getTopic() + "-" + message.getTag().orElse(null);
        log.info("收到一条消息：{}", key);
        String data = StandardCharsets.UTF_8.decode(message.getBody()).toString();
        var event = items1.get(key);
        if (event == null) {
            log.error("未注册消息对象{}, data:", key, data);
            return true;
        }
        return event.consume(data);
    }

    @Override
    public void onApplicationEvent(ApplicationContextEvent event) {
        if (event instanceof ContextRefreshedEvent) {
            ApplicationContext context = event.getApplicationContext();
            if (context.getParent() == null) {
                if (!ServerConfig.enableTaskService()) {
                    log.info("当前应用未启动消息服务与定时任务");
                    return;
                }
                Map<String, AbstractQueue> queues = context.getBeansOfType(AbstractQueue.class);
                queues.forEach((name, queue) -> queue.startService(this));
                startPush();
                log.info("成功注册的推送消息数量：" + queues.size());
            }
        } else if (event instanceof ContextClosedEvent) {
            ApplicationContext context = event.getApplicationContext();
            if (context.getParent() == null) {
                Map<String, AbstractQueue> queues = context.getBeansOfType(AbstractQueue.class);
                queues.forEach((name, queue) -> queue.stopService());
                log.info("关闭注册的推送消息数量：" + queues.size());
            }
        }
    }

    public void startPush() {
        if (pushConsumer != null) {
//            log.info("startPush 被执行");
            return;
        }
        if (items1.size() == 0)
            return;
        Client client = QueueServer.getClient();
        String groupId = getGroupId();
        try {
            // 查找指定的主题组是否存在
            GetConsumerGroupResponse response = client.getConsumerGroup(QueueServer.getInstanceId(), groupId);
            GetConsumerGroupResponseBodyData data = response.getBody().getData();
            if (data == null) {
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
                    log.error("创建消费组 {} 失败", groupId);
                    return;
                }
            }
        } catch (Exception e1) {
            log.error(e1.getMessage(), e1);
            return;
        }

        ClientConfiguration clientConfiguration = QueueServer.getConfig();
        try {
            final ClientServiceProvider provider = ClientServiceProvider.loadService();
            PushConsumerBuilder builder = provider.newPushConsumerBuilder();
            builder.setConsumerGroup(groupId);
            builder.setClientConfiguration(clientConfiguration);
            builder.setSubscriptionExpressions(items2);
            builder.setConsumptionThreadCount(5);
            builder.setMessageListener(
                    message -> this.consume(message) ? ConsumeResult.SUCCESS : ConsumeResult.FAILURE);
            this.pushConsumer = builder.build();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public int startPull() {
        if (pullConsumer == null) {
            final ClientServiceProvider provider = QueueServer.loadService();
            try {
                pullConsumer = provider.newSimpleConsumerBuilder()
                        .setClientConfiguration(QueueServer.getConfig())
                        .setConsumerGroup(this.getGroupId())
                        // 拉取时，等服务器多久
                        .setAwaitDuration(Duration.ofSeconds(5L))
                        // Set the subscription for the consumer.
                        .setSubscriptionExpressions(items2)
                        .build();
            } catch (ClientException e) {
                log.error(e.getMessage());
                e.printStackTrace();
                return 0;
            }
        }
        List<MessageView> messages;
        try {
            // 设置在未确认成功时，多长时间后再可见
            messages = pullConsumer.receive(100, Duration.ofSeconds(10));
            for (MessageView message : messages) {
                if (this.consume(message))
                    pullConsumer.ack(message);
            }
            return messages.size();
        } catch (ClientException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            return 0;
        }
    }

    public String getGroupId() {
        return String.format("%s-%s-%s", ServerConfig.getAppProduct(), ServerConfig.getAppIndustry(),
                ServerConfig.getAppVersion());
    }

}
