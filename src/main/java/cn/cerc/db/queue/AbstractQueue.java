package cn.cerc.db.queue;

public abstract class AbstractQueue implements AutoCloseable {

    protected final QueueConsumer consumer = QueueConsumer.create(this.getTopic(), QueueConfig.tag);

    public abstract String getTopic();

    @Override
    public void close() {
        consumer.close();
    }

    public AbstractQueue() {
        super();
        QueueServer.createTopic(this.getTopic());
    }

}
