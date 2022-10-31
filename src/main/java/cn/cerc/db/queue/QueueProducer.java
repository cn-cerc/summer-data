package cn.cerc.db.queue;

import java.io.IOException;
import java.time.Duration;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueProducer {
    private static final Logger log = LoggerFactory.getLogger(QueueProducer.class);
    private static Producer producer = QueueServer.getProducer();
    private ClientServiceProvider provider = QueueServer.loadService();
    private String topic;
    private String tag;

    public QueueProducer(String topic, String tag) {
        this.topic = topic;
        this.tag = tag;
    }

    public String append(String value, Duration delayTime) throws ClientException {
        // 普通消息发送
        var builder = provider.newMessageBuilder()
                .setTopic(topic)
                // 设置消息Tag，用于消费端根据指定Tag过滤消息。
                .setTag(tag)
                // 消息体。
                .setBody(value.getBytes());

        if (delayTime.getSeconds() > 0)
            builder.setDeliveryTimestamp(System.currentTimeMillis() + delayTime.getSeconds() * 1000);

        try {
            // 发送消息，需要关注发送结果，并捕获失败等异常。
            SendReceipt sendReceipt = producer.send(builder.build());
            return sendReceipt.getMessageId().toString();
        } catch (ClientException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public void close() {
        if (producer != null) {
            try {
                log.error("close producer from RecketMQ");
                producer.close();
                producer = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }
}
