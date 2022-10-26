package cn.cerc.db.queue;

import java.nio.charset.StandardCharsets;

import org.apache.rocketmq.client.apis.message.MessageView;

import cn.cerc.db.core.DataSet;

public class QueueQuery {
    private QueueConsumer consumer;
    private MessageView message;
    private String topic;

    public QueueQuery(String topic) {
        QueueServer.createTopic(topic);
        consumer = QueueConsumer.create(topic, QueueConfig.tag);
    }

    public DataSet open() {
        this.message = consumer.recevie();
        DataSet dataSet = new DataSet();
        dataSet.setJson(StandardCharsets.UTF_8.decode(message.getBody()).toString());
        return dataSet;
    }

    public void save(String json) {
        QueueServer.append(this.topic, QueueConfig.tag, json);
    }

    /**
     * @return 移除消息队列
     */
    public boolean remove() {
        consumer.delete(message);
        return true;
    }

}
