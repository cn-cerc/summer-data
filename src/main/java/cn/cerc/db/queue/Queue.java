package cn.cerc.db.queue;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.client.AsyncCallback;
import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.model.Message;

import cn.cerc.db.core.Datetime;

public class Queue implements AsyncCallback<List<Message>>, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Queue.class);

    private CloudQueue client;
    private QueueServer server = new QueueServer();

    public Queue(String queueId) {
        super();
        this.client = server.createQueue(queueId);
    }

    public Message read() {
        Message message = null;
        try {
            message = client.popMessage();
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
        client.putMessage(message);
    }

    public void delete(Message message) {
        if (message != null)
            client.deleteMessage(message.getReceiptHandle());
        return;
    }

    public void recevie() {
        client.asyncBatchPopMessage(10, 5, this);
    }

    @Override
    public void onSuccess(List<Message> result) {
        for (var message : result) {
            System.out.println("onSuccess: " + message.getMessageBody());
            this.delete(message);
        }
    }

    @Override
    public void onFail(Exception ex) {
        ex.printStackTrace();
        System.out.println("出错了: " + ex.getMessage());
    }

    @Override
    public void close() {
        server.close();
        server = null;
    }

    public static void main(String[] args) {
        Queue queue = new Queue("test");
//        for (int i = 1; i < 4; i++)
//            queue.send("val" + i);
        System.out.println("clear");
        Message msg = null;
        do {
            msg = queue.read();
            if (msg != null) {
                System.out.println(msg.getMessageBody());
                System.out.println(msg.getNextVisibleTime());
                queue.delete(msg);
            }
        } while (msg != null);
        System.out.println("start recevie: " + new Datetime());
        System.out.println("start send: " + new Datetime());
        for (int i = 0; i < 5; i++) {
            try {
                queue.send(new Datetime().toString());
                Thread.sleep(1000);
                queue.recevie();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        queue.close();
        System.out.println("end: " + new Datetime());
    }

}
