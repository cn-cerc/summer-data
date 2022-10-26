package cn.cerc.db.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.aliyun.mns.client.CloudAccount;
import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.model.Message;
import com.aliyun.mns.model.QueueMeta;

import cn.cerc.db.SummerDB;
import cn.cerc.db.core.ClassResource;
import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.IConnection;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueueServer implements IConnection {
    private static final ClassResource res = new ClassResource(QueueServer.class, SummerDB.ID);
    private static final Logger log = LoggerFactory.getLogger(QueueServer.class);

    public static final String AccountEndpoint = "mns.accountendpoint";
    public static final String AccessKeyId = "mns.accesskeyid";
    public static final String AccessKeySecret = "mns.accesskeysecret";
    public static final String RMQAccountEndpoint = "rocketmq.endpoint";
    public static final String RMQInstanceId = "rocketmq.instanceId";
    public static final String RMQAccessKeyId = "oss.accessKeyId";
    public static final String RMQAccessKeySecret = "oss.accessKeySecret";
//    public static final String SecurityToken = "mns.securitytoken";
    // IHandle中识别码
    public static final String SessionId = "aliyunQueueSession";

    // 默认不可见时间
    private static int visibilityTimeout = 50;
    private static MNSClient client;
    private static CloudAccount account;
    private static final IConfig config;

    static {
        config = ServerConfig.getInstance();
    }

    public static synchronized MNSClient getMNSClient() {
        if (client != null && client.isOpen())
            return client;

        if (account == null) {
            String endpoint = config.getProperty(QueueServer.AccountEndpoint, null);
            String accessId = config.getProperty(QueueServer.AccessKeyId, null);
            String password = config.getProperty(QueueServer.AccessKeySecret, null);
            if (endpoint == null)
                throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.AccountEndpoint));

            if (accessId == null)
                throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.AccessKeyId));

            if (password == null)
                throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.AccessKeySecret));

            if (account == null)
                account = new CloudAccount(accessId, password, endpoint);
        }
        if (client == null)
            client = account.getMNSClient();
        return client;

    }

    @Override
    public MNSClient getClient() {
        return getMNSClient();
    }

    /**
     * 根据队列的URL创建CloudQueue对象，后于后续对改对象的创建、查询等
     *
     * @param queueCode 队列代码
     * @return value 返回具体的消息队列
     */
    public static CloudQueue openQueue(String queueCode) {
        return getMNSClient().getQueueRef(queueCode);
    }

    /**
     * 创建队列
     *
     * @param queueCode 队列代码
     * @return value 返回创建的队列
     */
    public static CloudQueue createQueue(String queueCode) {
        QueueMeta meta = new QueueMeta();
        // 设置队列的名字
        meta.setQueueName(queueCode);
        // 设置队列消息的长轮询等待时间，0为关闭长轮询
        meta.setPollingWaitSeconds(0);
        // 设置队列消息的最大长度，单位是byte
        meta.setMaxMessageSize(65356L);
        // 设置队列消息的最大长度，单位是byte
        meta.setMessageRetentionPeriod(72000L);
        // 设置队列消息的不可见时间，即取出消息隐藏时长，单位是秒
        meta.setVisibilityTimeout(180L);
        return getMNSClient().createQueue(meta);
    }

    /**
     * 发送消息
     *
     * @param queue   消息队列
     * @param content 消息内容
     * @return value 返回值，当前均为true
     */
    public static boolean append(CloudQueue queue, String content) throws ServiceException, ClientException {
        Message message = new Message();
        message.setMessageBody(content);
        Message result = queue.putMessage(message);
        return !Utils.isEmpty(result.getMessageId());
    }

    /**
     * 获取队列中的消息
     *
     * @param queue 消息队列
     * @return value 返回请求的删除，可为null
     */
    public static Message receive(CloudQueue queue) {
        Message message = null;
        try {
            message = queue.popMessage();
            if (message != null) {
                log.debug("messageBody：{}", message.getMessageBodyAsString());
                log.debug("messageId：{}", message.getMessageId());
                log.debug("receiptHandle ：{}", message.getReceiptHandle());
            } else {
                log.debug("message  is null");
            }
        } catch (ServiceException | ClientException e) {
            log.debug(e.getMessage());
        }
        return message;
    }

    /**
     * 删除消息
     *
     * @param queue         队列
     * @param receiptHandle 消息句柄
     */
    public static void delete(CloudQueue queue, String receiptHandle) {
        queue.deleteMessage(receiptHandle);
    }

    /**
     * 查看队列消息
     *
     * @param queue 队列
     * @return value 返回取得的消息体
     */
    public static Message peek(CloudQueue queue) {
        return queue.peekMessage();
    }

    /**
     * 延长消息不可见时间
     *
     * @param queue         队列
     * @param receiptHandle 消息句柄
     */
    public static void changeVisibility(CloudQueue queue, String receiptHandle) {
        // 第一个参数为旧的ReceiptHandle值，第二个参数为新的不可见时间（VisibilityTimeout）
        String newReceiptHandle = queue.changeMessageVisibilityTimeout(receiptHandle, visibilityTimeout);
        log.debug("new receipt handle: " + newReceiptHandle);
    }

    public IConfig getConfig() {
        return config;
    }

    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    public static com.aliyun.rocketmq20220801.Client getRmqClient() throws Exception {
        String endpoint = config.getProperty(QueueServer.RMQAccountEndpoint, null);
        String accessId = config.getProperty(QueueServer.RMQAccessKeyId, null);
        String password = config.getProperty(QueueServer.RMQAccessKeySecret, null);
        if (endpoint == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.RMQAccountEndpoint));

        if (accessId == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.RMQAccessKeyId));

        if (password == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.RMQAccessKeySecret));
        com.aliyun.teaopenapi.models.Config config = new com.aliyun.teaopenapi.models.Config().setAccessKeyId(accessId)
                .setAccessKeySecret(password);
        // 访问的域password
        config.endpoint = endpoint;
        return new com.aliyun.rocketmq20220801.Client(config);
    }

    public static String getRmqInstanceId() {
        String instanceId = config.getProperty(QueueServer.RMQInstanceId, null);
        if (instanceId == null)
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), QueueServer.RMQInstanceId));
        return instanceId;
    }

}
