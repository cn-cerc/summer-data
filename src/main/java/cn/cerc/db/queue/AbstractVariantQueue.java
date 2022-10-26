package cn.cerc.db.queue;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.mns.model.Message;

import cn.cerc.db.core.Variant;

public abstract class AbstractVariantQueue extends AbstractQueue {

    public void append(String data) {
        super.send(data);
    }

    public Variant receive() {
        Message msg = this.popMessage();
        if (msg == null)
            return null;
        return new Variant(getMessageBody(msg)).setKey(msg.getReceiptHandle());
    }

    public void delete(Variant variant) {
        getQueue().deleteMessage(variant.key());
    }

    public List<Variant> receive(int maximum) {
        if (maximum <= 0)
            throw new RuntimeException("maximum 必须大于 0");
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
    }

}
