package cn.cerc.db.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.model.Message;

public abstract class AbstractQueue implements QueueImpl {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);

    private CloudQueue cloudQueue;

    @Override
    public abstract String getQueueId();

    @Override
    public CloudQueue getQueue() {
        if (this.cloudQueue == null)
            this.cloudQueue = QueueServer.getQueue(getQueueId());
        return cloudQueue;
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
