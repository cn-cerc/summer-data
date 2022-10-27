package cn.cerc.db.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.queue.QueueConsumer.OnMessageCallback;

public abstract class AbstractQueue {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);

    public abstract String getTopic();

    public abstract OnMessageCallback onMessage();

    public AbstractQueue() {
        log.info("Queue {} {} is init ", this.getClass().getSimpleName(), getTopic());
        QueueServer.createTopic(this.getTopic());
        QueueConsumer.create(this.getTopic(), QueueConfig.tag, onMessage());
    }

}
