package cn.cerc.db.queue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.rocketmq20220801.Client;
import com.aliyun.rocketmq20220801.models.CreateTopicRequest;
import com.aliyun.rocketmq20220801.models.CreateTopicResponse;
import com.aliyun.rocketmq20220801.models.ListTopicsRequest;
import com.aliyun.rocketmq20220801.models.ListTopicsResponse;
import com.aliyun.rocketmq20220801.models.ListTopicsResponseBody.ListTopicsResponseBodyDataList;
import com.aliyun.teaopenapi.models.Config;

import cn.cerc.db.SummerDB;
import cn.cerc.db.core.ClassResource;
import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.ServerConfig;

public class QueueServer {
    private static final ClassResource res = new ClassResource(QueueServer.class, SummerDB.ID);
    private static final Logger log = LoggerFactory.getLogger(QueueServer.class);

    public static final String AccountEndpoint = "mns.accountendpoint";
    public static final String AccessKeyId = "mns.accesskeyid";
    public static final String AccessKeySecret = "mns.accesskeysecret";
    public static final String RMQAccountEndpoint = "rocketmq.endpoint";
    public static final String RMQInstanceId = "rocketmq.instanceId";
    public static final String RMQEndpoint = "rocketmq.queue.endpoint";
    public static final String RMQAccessKeyId = "rocketmq.queue.accesskeyid";
    public static final String RMQAccessKeySecret = "rocketmq.queue.accesskeysecret";

    private static final IConfig config = ServerConfig.getInstance();

    private static final QueueProducer producer = new QueueProducer();

    public static QueueProducer producer() {
        return QueueServer.producer;
    }

    private static final List<String> queues = new ArrayList<>();

    public static void createTopic(String topic, boolean isDelayQueue) {
        if (queues.contains(topic))
            return;

        try {
            // TODO 临时先只加载100个，后需要改为全部加载
            // 载入所有的topic
            ListTopicsRequest request = new ListTopicsRequest();
            request.setPageNumber(1);
            request.setPageSize(100);

            ListTopicsResponse response = getClient().listTopics(QueueServer.getInstanceId(), request);
            List<ListTopicsResponseBodyDataList> list = response.getBody().getData().getList();
            boolean exists = false;
            if (list == null || list.size() == 0)
                exists = false;
            else
                exists = list.stream().anyMatch(item -> topic.equals(item.getTopicName()));
            if (exists) {
                queues.add(topic);
                return;
            }

            CreateTopicRequest createRequest = new CreateTopicRequest();
            if (!isDelayQueue)
                createRequest.setMessageType(MessageType.NORMAL.name());
            else
                createRequest.setMessageType(apache.rocketmq.v2.MessageType.DELAY.name());
            CreateTopicResponse createResponse = getClient().createTopic(QueueServer.getInstanceId(), topic,
                    createRequest);
            if (createResponse.getBody().getSuccess()) {
                queues.add(topic);
                log.info("current topic {}", queues.size());
                return;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static String append(String topic, String tag, String value, Duration delayTime) {
        try {
            return producer.append(topic, tag, value,delayTime);
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Client getRocketmqClient() {
        String endpoint = config.getProperty(QueueServer.RMQAccountEndpoint, null);
        if (endpoint == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.RMQAccountEndpoint));

        String accessId = config.getProperty(QueueServer.AccessKeyId, null);
        if (accessId == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.AccessKeyId));

        String password = config.getProperty(QueueServer.AccessKeySecret, null);
        if (password == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.AccessKeySecret));
        Config config = new Config().setAccessKeyId(accessId).setAccessKeySecret(password);

        config.endpoint = endpoint;
        try {
            return new Client(config);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    public static String getInstanceId() {
        String instanceId = config.getProperty(QueueServer.RMQInstanceId, null);
        if (instanceId == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.RMQInstanceId));
        return instanceId;
    }

    public static String getEndpoint() {
        String endpoint = config.getProperty(QueueServer.RMQEndpoint, null);
        if (endpoint == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.RMQEndpoint));
        return endpoint;
    }

    public static String getAccessKeyId() {
        String accessKeyId = config.getProperty(QueueServer.RMQAccessKeyId, null);
        if (accessKeyId == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.RMQAccessKeyId));
        return accessKeyId;
    }

    public static String getAccessSecret() {
        String accessKeySecret = config.getProperty(QueueServer.RMQAccessKeySecret, null);
        if (accessKeySecret == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.RMQAccessKeySecret));
        return accessKeySecret;
    }

    public static Client getClient() {
        log.info("{} get mq client ", Thread.currentThread());
        return getRocketmqClient();
    }

}
