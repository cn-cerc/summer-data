package cn.cerc.db.queue;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.queue.QueueConsumer.OnMessageCallback;

public abstract class AbstractQueue {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);
    private QueueConsumer consumer;
    private long delayTime = 0L;
    private String tag;

    public AbstractQueue() {
        super();
        log.info("Queue {} {} is init ", this.getClass().getSimpleName(), getTopic());
        this.tag = QueueConfig.tag;
        //
        QueueServer.createTopic(this.getTopic(), this.getDelayTime() > 0);
        this.consumer = QueueConsumer.create(this.getTopic(), this.getTag(), onMessage());
    }

    public abstract String getTopic();

    public abstract OnMessageCallback onMessage();

    protected String sendMessage(String data) {
        return QueueServer.append(getTopic(), getTag(), data, Duration.ofSeconds(this.delayTime));
    }

    public long getDelayTime() {
        return this.delayTime;
    }

    public AbstractQueue setDelayTime(long delayTime) {
        this.delayTime = delayTime;
        return this;
    }

    public AbstractQueue setTag(String tag) {
        this.tag = tag;
        return this;
    }

    protected String getTag() {
        return this.tag;
    }

    public QueueConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(QueueConsumer consumer) {
        this.consumer = consumer;
    }

}
