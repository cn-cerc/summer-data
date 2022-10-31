package cn.cerc.db.queue;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.producer.Producer;
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
import cn.cerc.db.zk.ZkConfig;

public class QueueServer {
    private static final ClassResource res = new ClassResource(QueueServer.class, SummerDB.ID);
    private static final Logger log = LoggerFactory.getLogger(QueueServer.class);

//    public static final String AccountEndpoint = "mns.accountendpoint";
    public static final String AliyunAccessKeyId = "aliyunAccesskeyid";
    public static final String AliyunAccessKeySecret = "aliyunAccesskeysecret";

    public static final String RMQAccountEndpoint = "accountEndpoint";
    public static final String RMQInstanceId = "instanceId";
    public static final String RMQEndpoint = "endpoint";
    public static final String RMQAccessKeyId = "accessKeyId";
    public static final String RMQAccessKeySecret = "accessKeySecret";

    private static final IConfig config = new ZkConfig("/rocketMQ");

    private static final List<String> queues = new ArrayList<>();

    private static ClientServiceProvider provider;
    private static Client client;
    private static ClientConfiguration clientConfig;
    private static Producer producer;

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

            log.info("create topic request");
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

    private static Client getRocketmqClient() {
        if (client != null)
            return client;

        String endpoint = config.getProperty(QueueServer.RMQAccountEndpoint, null);
        if (endpoint == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.RMQAccountEndpoint));

        String accessId = config.getProperty(QueueServer.AliyunAccessKeyId, null);
        if (accessId == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.AliyunAccessKeyId));

        String password = config.getProperty(QueueServer.AliyunAccessKeySecret, null);
        if (password == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.AliyunAccessKeySecret));
        Config config = new Config().setAccessKeyId(accessId).setAccessKeySecret(password);
        config.endpoint = endpoint;
        try {
            client = new Client(config);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return client;
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

    public synchronized static Client getClient() {
        log.debug("{} get client from RocketMQ", Thread.currentThread());
        return getRocketmqClient();
    }

    public static synchronized ClientServiceProvider loadService() {
        if (provider == null)
            return ClientServiceProvider.loadService();
        return provider;
    }

    public synchronized static ClientConfiguration getConfig() {
        if (clientConfig != null)
            return clientConfig;
        loadService();
        var credentialsProvider = new StaticSessionCredentialsProvider(QueueServer.getAccessKeyId(),
                QueueServer.getAccessSecret());
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder()
                .setEndpoints(QueueServer.getEndpoint())
                .setCredentialProvider(credentialsProvider);
        clientConfig = builder.build();
        return clientConfig;
    }

    public synchronized static Producer getProducer() {
        if (producer == null) {
            var configuration = QueueServer.getConfig();
            var provider = QueueServer.loadService();
            try {
                producer = provider.newProducerBuilder().setClientConfiguration(configuration).build();
            } catch (ClientException e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }
        return producer;
    }

}
