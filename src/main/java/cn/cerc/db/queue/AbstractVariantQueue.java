package cn.cerc.db.queue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;

import cn.cerc.db.core.Variant;

public abstract class AbstractVariantQueue extends AbstractQueue {

    private transient Map<Variant, MessageView> rmqItems = new HashMap<>();

    public void append(String data) throws ClientException {
        QueueServer.append(getTopic(), QueueConfig.tag, data);
    }

    public Variant receive() {
        var msg = consumer.recevie();
        if (msg == null)
            return null;
        Variant variant = new Variant(StandardCharsets.UTF_8.decode(msg.getBody()).toString())
                .setKey(msg.getMessageId().toString());
        rmqItems.put(variant, msg);
        return variant;
    }

    public void delete(Variant variant) {
        if (!rmqItems.containsKey(variant))
            throw new RuntimeException("variant not find!");
        var message = rmqItems.get(variant);
        if (message != null) {
            consumer.delete(message);
            rmqItems.remove(variant);
        }
    }

    public List<Variant> receive(int maximum) {
        if (maximum <= 0)
            throw new RuntimeException("maximum 必须大于 0");

        List<Variant> items = new ArrayList<>();
        int total = 0;
        var msg = consumer.recevie();
        while (msg != null) {
            total++;
            items.add(new Variant(StandardCharsets.UTF_8.decode(msg.getBody()).toString())
                    .setKey(msg.getMessageId().toString()));
            if (total == maximum)
                break;
            msg = consumer.recevie();
        }
        return items;
    }

}
