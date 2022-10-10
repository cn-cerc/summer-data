package cn.cerc.db.queue;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.CloudTopic;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.model.Base64TopicMessage;
import com.aliyun.mns.model.PagingListResult;
import com.aliyun.mns.model.QueueMeta;
import com.aliyun.mns.model.SubscriptionMeta;
import com.aliyun.mns.model.TopicMessage;
import com.aliyun.mns.model.TopicMeta;

import cn.cerc.db.queue.QueueServer;

public class Topic implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Topic.class);
    private MNSClient client;
    private String topicName;
    private CloudTopic topic;
    private List<String> queueList = new ArrayList<>();

    public Topic(String topicName) {
        super();
        client = new QueueServer().getClient();
        this.topicName = topicName;
//        this.topic = client.getTopicRef(topicName);
        TopicMeta meta = new TopicMeta();
        meta.setTopicName(topicName);
        this.topic = client.createTopic(meta);
        log.debug("topicName: " + topic);
    }

    public void addSubscribe(String queue) {
        queueList.add(queue);
        createQueue(this.topicName + "-" + queue, 30);
        createSubscribe(this.topicName + "-" + queue);
    }

    private boolean createQueue(String queueName, int pollingWaitseconds) {
        try {
            String name = this.topicName + "-" + queueName;
            PagingListResult<QueueMeta> listQueue = client.listQueue(name, "", 100);
            if (listQueue != null) {
                for (var item : listQueue.getResult()) {
                    if (item.getQueueName().equals(queueName))
                        return false;
                }
            }
            QueueMeta qMeta = new QueueMeta();
            qMeta.setQueueName(queueName);
            qMeta.setPollingWaitSeconds(pollingWaitseconds);
            CloudQueue cQueue = client.createQueue(qMeta);
            log.debug("Create queue successfully. URL: " + cQueue.getQueueURL());
            return true;
        } catch (ClientException ce) {
            log.debug("Something wrong with the network connection between client and MNS service."
                    + "Please check your network and DNS availability.");
            ce.printStackTrace();
        } catch (ServiceException se) {
            if (se.getErrorCode().equals("QueueNotExist")) {
                log.debug("Queue is not exist.Please create before use");
            } else if (se.getErrorCode().equals("TimeExpired")) {
                log.debug("The request is time expired. Please check your local machine timeclock");
            }
            se.printStackTrace();
        } catch (Exception e) {
            log.debug("Unknown exception happened!");
            e.printStackTrace();
        }
        return false;
    }

    private String createSubscribe(String queueName) {
        String region = "cn-shenzhen";
        String accountId = "1914523181140617";
        try {
            SubscriptionMeta meta = new SubscriptionMeta();
            meta.setSubscriptionName(queueName + "-subscribe");
            meta.setEndpoint(String.format("acs:mns:%s:%s:queues/%s", region, accountId, queueName));
            meta.setNotifyContentFormat(SubscriptionMeta.NotifyContentFormat.XML);
            PagingListResult<SubscriptionMeta> listSubscriptions = topic.listSubscriptions(this.topicName, "", 100);
            if (listSubscriptions != null) {
                for (var item : listSubscriptions.getResult()) {
                    if (meta.getSubscriptionName().equals(item.getSubscriptionName()))
                        return "";
                }
            }
            String subUrl = topic.subscribe(meta);
            log.debug("subscription url: " + subUrl);
            return subUrl;
        } catch (Exception e) {
            e.printStackTrace();
            log.debug("subscribe/unsubribe error");
            return null;
        }
    }

    /**
     * 消息生产逻辑
     *
     * @param message
     * @return null 为成功，否则为失败
     */
    public String publish(String message) {
        Base64TopicMessage msg = new Base64TopicMessage();
        msg.setMessageBody(message);
        String body = msg.getMessageBody();
        if (body == null || body.trim().length() == 0)
            return "msg's body is empty";
        try {
            TopicMessage result = topic.publishMessage(msg);
            return result.getMessageId();
        } catch (ClientException ce) {
            return "mns client exception : " + ce.toString();
        } catch (ServiceException se) {
            return "mns server exception : " + se.toString();
        } catch (Exception e) {
            return "mns unknown exception happened!: " + e.getMessage();
        }
    }

    /**
     * 删除主题
     */
    public void delete() {
        topic.delete();
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

}
