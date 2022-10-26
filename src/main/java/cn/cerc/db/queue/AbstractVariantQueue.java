package cn.cerc.db.queue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;

import com.aliyun.mns.model.Message;

import cn.cerc.db.core.Variant;

public abstract class AbstractVariantQueue extends AbstractQueue {

    private transient Map<Variant, MessageView> rmqItems = new HashMap<>();

    public void append(String data) throws ClientException {
        if (rmqQueue == null) {
            Message message = new Message();
            message.setMessageBody(data);
            getQueue().putMessage(message);
        } else {
            rmqQueue.producer().append(data);
        }
    }

    public Variant receive() throws ClientException {
        if (rmqQueue == null) {
            Message msg = this.popMessage();
            if (msg == null)
                return null;
            return new Variant(getMessageBody(msg)).setKey(msg.getReceiptHandle());
        } else {
//            StandardCharsets.UTF_8.decode(msg.getBody()).toString()
            var msg = rmqQueue.consumer().recevie();
            if (msg == null)
                return null;
            Variant variant = new Variant(StandardCharsets.UTF_8.decode(msg.getBody()).toString())
                    .setKey(msg.getMessageId().toString());
            rmqItems.put(variant, msg);
            return variant;
        }
    }

    public void delete(Variant variant) throws ClientException {
        if (rmqQueue == null) {
            getQueue().deleteMessage(variant.key());
        } else {
            if (!rmqItems.containsKey(variant))
                throw new RuntimeException("variant not find!");
            var message = rmqItems.get(variant);
            if (message != null) {
                rmqQueue.consumer().ack(message);
                rmqItems.remove(variant);
            }
        }
    }

    public List<Variant> receive(int maximum) throws ClientException {
        if (maximum <= 0)
            throw new RuntimeException("maximum 必须大于 0");

        if (rmqQueue == null) {
            List<Variant> items = new ArrayList<>();
            int total = 0;
            Variant msg = this.receive();
            while (msg != null) {
                total++;
                items.add(msg);
                if (total == maximum)
                    break;
                msg = this.receive();
            }
            return items;
        } else {
            List<Variant> items = new ArrayList<>();
            int total = 0;
            var msg = rmqQueue.consumer().recevie();
            while (msg != null) {
                total++;
                items.add(new Variant(StandardCharsets.UTF_8.decode(msg.getBody()).toString())
                        .setKey(msg.getMessageId().toString()));
                if (total == maximum)
                    break;
                msg = rmqQueue.consumer().recevie();
            }
            return items;
        }

    }

}
