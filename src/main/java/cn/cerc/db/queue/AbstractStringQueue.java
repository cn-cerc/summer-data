package cn.cerc.db.queue;

import cn.cerc.db.queue.QueueConsumer.OnMessageCallback;

public abstract class AbstractStringQueue extends AbstractQueue {

    public void append(String data) {
        super.sendMessage(data);
    }

    public boolean receive(OnMessageCallback event) {
        QueueConsumer consumer = QueueConsumer.getConsumer(this.getTopic(), this.getTag());
        return consumer.receive(event);
    }
}
