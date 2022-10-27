package cn.cerc.db.queue;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;

import cn.cerc.db.core.Datetime;

public class QueueProducer {
    private static final ClientServiceProvider provider = ClientServiceProvider.loadService();
    private static volatile Producer producer;

    public QueueProducer() {
        if (producer == null) {
            synchronized (QueueProducer.class) {
                // 消息发送的目标Topic名称，需要提前在控制台创建，如果不创建直接使用会返回报错。
                ClientConfigurationBuilder builder = ClientConfiguration.newBuilder()
                        .setEndpoints(QueueServer.getEndpoint())
                        .setCredentialProvider(new StaticSessionCredentialsProvider(QueueServer.getAccessKeyId(),
                                QueueServer.getAccessSecret()));
                ClientConfiguration configuration = builder.build();
                try {
                    producer = provider.newProducerBuilder().setClientConfiguration(configuration).build();
                } catch (ClientException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public String append(String topic, String tag, String value) throws ClientException {
        // 普通消息发送。
        Message message = provider.newMessageBuilder()
                .setTopic(topic)
                // 设置消息Tag，用于消费端根据指定Tag过滤消息。
                .setTag(tag)
                // 消息体。
                .setBody(value.getBytes())
                .build();
        try {
            // 发送消息，需要关注发送结果，并捕获失败等异常。
            SendReceipt sendReceipt = producer.send(message);
            return sendReceipt.getMessageId().toString();
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws ClientException {
        QueueProducer producer = new QueueProducer();
        List<String> tags = new ArrayList<>();
        tags.add("a");
        tags.add("b");
        tags.add("c");
        tags.add("d");
        tags.add("e");

        for (int i = 0; i < 100; i++) {
            tags.forEach(tag -> {
                try {
                    String result = producer.append("test", tag, new Datetime().toString());
                    System.out.println("消息发送成功：" + result);
                } catch (ClientException e) {
                    e.printStackTrace();
                }
            });
        }

    }
}
