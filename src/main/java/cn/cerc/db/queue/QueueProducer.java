package cn.cerc.db.queue;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.SendResult;

public class QueueProducer {
    private Producer producer;
    private String topic;
    private String tag = "";

    public QueueProducer() {
        super();
        producer = ONSFactory.createProducer(RocketMQ.getProperties());
        // 在发送消息前，必须调用start方法来启动Producer，只需调用一次即可。
        producer.start();
    }

    public QueueProducer(Producer producer) {
        super();
        this.producer = producer;
    }

    public String append(String tag, String value) {
        // 循环发送消息。
        Message msg = new Message(topic, tag, value.getBytes());
        // 设置代表消息的业务关键属性，请尽可能全局唯一。
        // 以方便您在无法正常收到消息情况下，可通过消息队列RocketMQ版控制台查询消息并补发。
        // 注意：不设置也不会影响消息正常收发。
        msg.setKey(""); // 此处可以用来设置单号
        // 发送消息，需要关注发送结果，并捕获失败等异常。
        SendResult sendReceipt = producer.send(msg);
        return sendReceipt.getMessageId();
    }

    public void close() {
        if (producer != null) {
            producer.shutdown();
            producer = null;
        }
    }

    public String getTopic() {
        return topic;
    }

    public QueueProducer setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public QueueProducer setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public static void main(String[] args) {
        QueueProducer producer = new QueueProducer().setTopic("TopicTestMQ").setTag("fpl");
        var result = producer.append("fpl", "hello world");
        producer.close();
        System.out.println("消息发送成功：" + result);

    }
}
