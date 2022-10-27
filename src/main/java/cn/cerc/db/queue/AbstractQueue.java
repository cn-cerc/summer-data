package cn.cerc.db.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractQueue {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);
    protected QueueConsumer consumer;

    public abstract String getTopic();

    public AbstractQueue() {
        super();
        log.info("Queue {} {} is init ", this.getClass().getSimpleName(), getTopic());
        QueueServer.createTopic(this.getTopic());
        this.consumer = QueueConsumer.create(this.getTopic(), QueueConfig.tag);
    }

}
