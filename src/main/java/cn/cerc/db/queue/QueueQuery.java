package cn.cerc.db.queue;

import java.nio.charset.StandardCharsets;

import org.apache.rocketmq.client.apis.message.MessageView;

import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.Utils;

public class QueueQuery extends DataSet implements AutoCloseable {
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
        this.setJson(StandardCharsets.UTF_8.decode(message.getBody()).toString());
        return this;
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

    @Override
    public void close() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
    }

}
