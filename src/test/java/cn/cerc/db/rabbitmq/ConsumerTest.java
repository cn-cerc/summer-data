package cn.cerc.db.rabbitmq;

import java.io.IOException;

import org.junit.Test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class ConsumerTest {

    @Test
    public void test() throws IOException, InterruptedException {
        try (RabbitMQ rabbit = new RabbitMQ()) {
            Channel channel = rabbit.getChannel();
            channel.queueDeclare("work", false, false, false, null);
            channel.basicConsume("work", true, new DefaultConsumer(channel) {

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
                        byte[] body) throws IOException {
                    System.out.println(new String(body));
                }

            });
            System.out.println("等待消息消费");
            Thread.sleep(100000);
            System.out.println("结束等待");
        }
    }

}
