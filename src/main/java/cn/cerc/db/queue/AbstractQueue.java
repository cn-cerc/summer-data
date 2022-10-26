package cn.cerc.db.queue;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.java.message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.model.Message;
import com.aliyun.mns.model.PagingListResult;
import com.aliyun.mns.model.QueueMeta;
import com.aliyun.rocketmq20220801.Client;
import com.aliyun.rocketmq20220801.models.CreateTopicResponse;
import com.aliyun.rocketmq20220801.models.ListTopicsResponse;

public abstract class AbstractQueue implements QueueImpl {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);
    private static List<String> created = new ArrayList<>();
    private CloudQueue cloudQueue;
    protected RmqQueue rmqQueue;

    @Override
    public abstract String getQueueId();

    @Override
    public CloudQueue getQueue() {
        if (this.cloudQueue == null)
            this.cloudQueue = createQueue(this, getQueueId());
        return cloudQueue;
    }

    @Override
    public RmqQueue getRmqQueue() throws Exception {
        if (this.rmqQueue == null)
            this.rmqQueue = createRmqQueue(this, getQueueId());
        return rmqQueue;
    }

    private synchronized static CloudQueue createQueue(AbstractQueue sender, String queueName) {
        MNSClient client = QueueServer.getMNSClient();
        if (created.contains(queueName)) {
            log.debug("直接返回消息队列 {}", queueName);
            return client.getQueueRef(queueName);
        }
        // 先查找队列是否有建立，若有建立直接返回
        PagingListResult<QueueMeta> list = client.listQueue(queueName, "", 100);
        if (list != null) {
            for (var item : list.getResult()) {
                if (item.getQueueName().equals(queueName)) {
                    created.add(queueName);
                    log.debug("查找并返回消息队列 {}", queueName);
                    return client.getQueueRef(queueName);
                }
            }
        }
        QueueMeta meta = new QueueMeta();
        // 设置队列的名字
        meta.setQueueName(queueName);
        // 设置队列的属性
        sender.onCreateQueue(meta);
        created.add(queueName);
        log.debug("创建新的消息队列 {}", queueName);
        return client.createQueue(meta);
    }

    private synchronized static RmqQueue createRmqQueue(AbstractQueue sender, String queueName) throws Exception {
        Client rmqClient = QueueServer.getRmqClient();
        if (created.contains(queueName)) {
            log.debug("直接返回消息队列 {}", queueName);
            return new RmqQueue(queueName);
        }
        com.aliyun.rocketmq20220801.models.ListTopicsRequest listTopicRequest = new com.aliyun.rocketmq20220801.models.ListTopicsRequest();
        try {
            listTopicRequest.setPageNumber(1);
            listTopicRequest.setPageSize(100);
            // 复制代码运行请自行打印 API 的返回值
            ListTopicsResponse topicsResponse = rmqClient.listTopics(QueueServer.getRmqInstanceId(), listTopicRequest);
            boolean exists = topicsResponse.getBody()
                    .getData()
                    .getList()
                    .stream()
                    .anyMatch(item -> queueName.equals(item.getTopicName()));
            if (exists) {
                return new RmqQueue(queueName);
            } else {
                com.aliyun.rocketmq20220801.models.CreateTopicRequest createTopicRequest = new com.aliyun.rocketmq20220801.models.CreateTopicRequest();
                createTopicRequest.setMessageType(MessageType.NORMAL.name());
                CreateTopicResponse createTopicResponse = rmqClient.createTopic(QueueServer.getRmqInstanceId(),
                        queueName, createTopicRequest);
                if (createTopicResponse.getBody().getSuccess()) {
                    return new RmqQueue(queueName);
                }
                return null;
            }
        } catch (Exception _error) {
            throw _error;
        }
    }

    protected void onCreateQueue(QueueMeta meta) {
        // 设置队列消息的长轮询等待时间，0为关闭长轮询
        meta.setPollingWaitSeconds(0);
        // 设置队列消息的最大长度，单位是byte
        meta.setMaxMessageSize(65356L);
        // 设置队列消息的最大长度，单位是byte
        meta.setMessageRetentionPeriod(72000L);
        // 设置队列消息的不可见时间，即取出消息隐藏时长，单位是秒
        meta.setVisibilityTimeout(180L);
    }

    protected String getMessageBody(Message msg) {
        return msg.getMessageBody();
    }

    protected Message popMessage() {
        Message message = null;
        try {
            message = getQueue().popMessage();
            if (message != null) {
                log.debug("messageBody：{}", message.getMessageBodyAsString());
                log.debug("messageId：{}", message.getMessageId());
                log.debug("receiptHandle：{}", message.getReceiptHandle());
                log.debug(message.getMessageBody());
                return message;
            }
        } catch (ClientException e) {
            if (e.getMessage().indexOf("返回结果无效，无法解析。") > -1)
                return null;
            System.out.println("执行异常：" + e.getMessage());
        }
        return null;
    }
}
