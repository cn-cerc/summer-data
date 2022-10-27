package cn.cerc.db.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractQueue implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);
    protected final QueueConsumer consumer = QueueConsumer.create(this.getTopic(), QueueConfig.tag);

    public abstract String getTopic();

    @Override
    public void close() {
        log.error("{} {} is close ", this.getClass().getSimpleName(), getTopic());
        consumer.close();
    }

    public AbstractQueue() {
        super();
        log.error("{} {} is init ", this.getClass().getSimpleName(), getTopic());
        QueueServer.createTopic(this.getTopic());
    }

}
