package cn.cerc.db.queue;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.model.Message;
import com.aliyun.mns.model.PagingListResult;
import com.aliyun.mns.model.QueueMeta;
import com.aliyun.mns.model.RawTopicMessage;
import com.aliyun.mns.model.SubscriptionMeta;
import com.aliyun.mns.model.TopicMeta;

public abstract class AbstractQueue implements QueueImpl {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);
    private static List<String> created = new ArrayList<>();
    private CloudQueue cloudQueue;
    private String tag;
    private static final List<String> vers = List.of("csp", "fpl", "oem", "odm", "obm");

    @Override
    public abstract String getQueueId();

    @Override
    public CloudQueue getQueue() {
        if (this.cloudQueue == null) {
            String queueName = this.getQueueId();
            if (tag != null) {
                createTopic(queueName, tag);
            }
            this.cloudQueue = createQueue(this, queueName);
        }
        return cloudQueue;
    }

    private synchronized static void createTopic(String queueName, String tag) {
        String topicName = queueName + "-topic";
        MNSClient client = QueueServer.getMNSClient();
        PagingListResult<TopicMeta> list = client.listTopic(topicName, "", 10);
        if (list == null || list.getResult().size() == 0) {
            var topicMeta = new TopicMeta();
            topicMeta.setTopicName(topicName);
            var topic = client.createTopic(topicMeta);
            for (var ver : vers) {
                var meta = new SubscriptionMeta();
                meta.setEndpoint(QueueServer.endpoint);
                meta.setTopicName(topicName);
                meta.setFilterTag(tag);
                meta.setSubscriptionName(queueName + "-" + ver);
                topic.subscribe(meta);
            }
        }
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

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    protected void send(String value) {
        if (this.getTag() != null) {
            String topicName = this.getQueueId() + "-topic";
            MNSClient client = QueueServer.getMNSClient();
            createTopic(getQueueId(), this.tag);
            var topic = client.getTopicRef(topicName);
            System.out.println(topic);
            var msg = new RawTopicMessage();
            msg.setMessageBody(value);
            topic.publishMessage(msg);
        } else {
            Message message = new Message();
            message.setMessageBody(value);
            getQueue().putMessage(message);
        }
    }
}
