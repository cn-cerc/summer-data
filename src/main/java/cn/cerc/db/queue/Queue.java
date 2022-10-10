package cn.cerc.db.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.model.Message;

public class Queue implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Queue.class);

    private QueueServer server;
    private CloudQueue queue;

    public Queue(String queueId) {
        this(queueId, true);
    }

    public Queue(String queueId, boolean autoCreate) {
        super();
        this.server = new QueueServer();
        if (autoCreate)
            this.queue = server.createQueue(queueId);
        else
            this.queue = server.openQueue(queueId);
    }

    public Message read() {
        Message message = null;
        try {
            message = queue.popMessage();
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

    public void send(String content) {
        Message message = new Message();
        message.setMessageBody(content);
        queue.putMessage(message);
    }

    public void delete(Message message) {
        if (message != null)
            queue.deleteMessage(message.getReceiptHandle());
        return;
    }

    @Override
    public void close() {
        server.close();
        server = null;
    }

}
