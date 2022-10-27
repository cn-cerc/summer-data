package cn.cerc.db.queue;

import java.nio.charset.StandardCharsets;

import org.apache.rocketmq.client.apis.message.MessageView;

import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.Utils;

public class QueueQuery extends DataSet {
    private static final long serialVersionUID = 7781788221337787366L;

    private transient QueueConsumer consumer;
    private transient MessageView message;
    private transient String topic;

    public QueueQuery(String topic) {
        QueueServer.createTopic(topic);
        consumer = QueueConsumer.create(topic, QueueConfig.tag);
    }

    public QueueQuery open() {
        this.message = consumer.recevie();
        if (message == null)
            return this;
        this.setJson(StandardCharsets.UTF_8.decode(message.getBody()).toString());
        return this;
    }

    public boolean exists() {
        return message != null;
    }

    public String save(String json) {
        return QueueServer.append(this.topic, QueueConfig.tag, json);
    }

    /**
     * @return 移除消息队列
     */
    public boolean remove() {
        consumer.delete(message);
        message = null;
        return true;
    }

    public MessageView getMessage() {
        return message;
    }

    @Override
    public String json() {
        return new DataSetGson<>(this).encode();
    }

    @Override
    public QueueQuery setJson(String json) {
        super.clear();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        return this;
    }

}
