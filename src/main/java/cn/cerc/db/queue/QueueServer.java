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
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.zk.ZkNode;

public class QueueServer {
    private static final Logger log = LoggerFactory.getLogger(QueueServer.class);
    private static final List<String> queues = new ArrayList<>();
    private static ClientServiceProvider provider;
    private static Client client;
    private static ClientConfiguration clientConfig;
    private static Producer producer;
    private static final ClassResource res = new ClassResource(QueueServer.class, SummerDB.ID);
    private static ServerConfig config = ServerConfig.getInstance();
    private static final String RMQAccountEndpoint = "rocketMQ/accountEndpoint";
    private static final String RMQInstanceId = "rocketMQ/instanceId";

    private static final String RMQEndpoint = "rocketMQ/endpoint";
    private static final String RMQAccessKeyId = "rocketMQ/accessKeyId";
    private static final String RMQAccessKeySecret = "rocketMQ/accessKeySecret";

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

        Config config = new Config().setAccessKeyId(AliyunConfig.accessKeyId())
                .setAccessKeySecret(AliyunConfig.accessKeySecret());
        config.endpoint = QueueServer.getAccountEndpoint();
        try {
            client = new Client(config);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return client;
    }

    public static String getAccountEndpoint() {
        var result = ZkNode.get().getString(RMQAccountEndpoint, config.getProperty("rocketmq.endpoint"));
        if (Utils.isEmpty(result))
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), RMQAccountEndpoint));
        return result;
    }

    public static String getInstanceId() {
        var result = ZkNode.get().getString(RMQInstanceId, config.getProperty("rocketmq.instanceId"));
        if (Utils.isEmpty(result))
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), RMQInstanceId));
        return result;
    }

    public static String getEndpoint() {
        var result = ZkNode.get().getString(RMQEndpoint, config.getProperty("rocketmq.queue.endpoint"));
        if (Utils.isEmpty(result))
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), RMQEndpoint));
        return result;
    }

    public static String getAccessKeyId() {
        var result = ZkNode.get().getString(RMQAccessKeyId, config.getProperty("rocketmq.queue.accesskeyid"));
        if (Utils.isEmpty(result))
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.RMQAccessKeyId));
        return result;
    }

    public static String getAccessKeySecret() {
        var result = ZkNode.get().getString(RMQAccessKeySecret, config.getProperty("rocketmq.queue.accesskeysecret"));
        if (Utils.isEmpty(result))
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), RMQAccessKeySecret));
        return result;
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
                QueueServer.getAccessKeySecret());
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
