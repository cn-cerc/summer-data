package cn.cerc.db.queue;

public abstract class AbstractStringQueue extends AbstractQueue {

    public void append(String data) {
        super.sendMessage(getTopic(), getTag(), data);
    }

    public void append(String tag, String data) {
        super.sendMessage(getTopic(), tag, data);
    }

    public void append(String topic, String tag, String data) {
        super.sendMessage(topic, tag, data);
    }

//    public boolean receive(OnStringMessage event) {
//        QueueConsumer consumer = QueueConsumer.getConsumer(this.getTopic(), this.getTag());
//        return consumer.receive(event);
//    }
}
