package cn.cerc.db.queue.mns;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.model.Message;

import cn.cerc.db.core.Variant;
import cn.cerc.db.queue.OnStringMessage;

public class MnsQueue {
    private static final Logger log = LoggerFactory.getLogger(MnsQueue.class);
    private CloudQueue client;

    public MnsQueue(CloudQueue client) {
        this.client = client;
    }

    public String push(String content) {
        var message = new Message();
        message.setMessageBody(content);
        return client.putMessage(message).getMessageId();
    }

    public Variant pop() {
        Message message = null;
        try {
            message = client.popMessage();
        } catch (ServiceException | ClientException e) {
            log.error(e.getMessage(), e);
        }
        if (message == null)
            return null;
        return new Variant(message.getMessageBody()).setKey(message.getReceiptHandle());
    }

    public void delete(Variant item) {
        client.deleteMessage(item.key());
    }

    public int pop(int batchMax, OnStringMessage msg) {
        var item = this.pop();
        var total = 0;
        while (item != null && total < batchMax) {
            if (msg.consume(item.getString(), true)) {
                this.delete(item);
                log.info("delete message key {}, {}", item.key(), item.getString());
            }
            if (++total < batchMax)
                item = this.pop();
        }
        return total;
    }

}
