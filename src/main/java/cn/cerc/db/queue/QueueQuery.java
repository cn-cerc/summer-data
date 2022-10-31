package cn.cerc.db.queue;

import java.time.Duration;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.Utils;

public class QueueQuery extends DataSet {
    private static final long serialVersionUID = 7781788221337787366L;
    private static final Logger log = LoggerFactory.getLogger(QueueQuery.class);
    private transient MessageView message;
    private transient String topic;

    public QueueQuery(String topic) {
        this.topic = topic;
        QueueServer.createTopic(topic, false);
    }

    public QueueQuery open() {
        var self = this;
//        QueueConsumer consumer = QueueConsumer.getConsumer(topic, QueueConfig.tag);
//        consumer.receive(message -> {
//            self.setJson(message);
//            return true;
//        });
        return this;
    }

    public boolean exists() {
        return message != null;
    }

    public void save(String json) {
        try  {
            var queue = new QueueProducer(this.topic, QueueConfig.tag);
            queue.append(json, Duration.ZERO);
        } catch (ClientException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
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
