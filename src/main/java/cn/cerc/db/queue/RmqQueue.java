package cn.cerc.db.queue;

import org.apache.rocketmq.client.apis.ClientException;

import cn.cerc.db.core.ServerConfig;

public class RmqQueue {

    private final static String TAG = ServerConfig.getInstance().getProperty("version");

    private String topic;

    private QueueProducer producer;

    private QueueConsumer consumer;

    public RmqQueue(String topic) throws ClientException {
        this.topic = topic;
        this.producer = new QueueProducer().setTopic(this.topic).setTag(TAG);
        this.consumer = new QueueConsumer(this.topic, TAG);
    }

    public QueueProducer producer() {
        return producer;
    }

    public QueueConsumer consumer() {
        return consumer;
    }

}
