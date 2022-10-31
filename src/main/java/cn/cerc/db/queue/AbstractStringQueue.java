package cn.cerc.db.queue;

public abstract class AbstractStringQueue extends AbstractQueue {

    public void append(String data) {
        super.sendMessage(data);
    }

//    public boolean receive(OnStringMessage event) {
//        QueueConsumer consumer = QueueConsumer.getConsumer(this.getTopic(), this.getTag());
//        return consumer.receive(event);
//    }
}
