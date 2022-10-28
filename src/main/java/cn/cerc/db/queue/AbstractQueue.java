package cn.cerc.db.queue;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.queue.QueueConsumer.OnMessageCallback;

public abstract class AbstractQueue {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);

    protected static final Map<Class<? extends AbstractQueue>, QueueConsumer> consumers = new HashMap<>();

    protected QueueConsumer consumer;

    public abstract String getTopic();

    public abstract OnMessageCallback onMessage();

    public AbstractQueue() {
        log.info("Queue {} {} is init ", this.getClass().getSimpleName(), getTopic());
        QueueServer.createTopic(this.getTopic());
        if (consumers.containsKey(getClass()))
            this.consumer = consumers.get(getClass());
        else {
            QueueConsumer consumer = QueueConsumer.create(this.getTopic(), QueueConfig.tag, onMessage());
            this.consumer = consumer;
            consumers.put(getClass(), consumer);
        }
    }

}
