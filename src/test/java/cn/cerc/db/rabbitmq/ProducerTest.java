package cn.cerc.db.rabbitmq;

import java.io.IOException;

import org.junit.Test;

import com.rabbitmq.client.Channel;

public abstract class ProducerTest {

    @Test
    public void test() throws IOException, InterruptedException {
        try (RabbitMQ rabbit = new RabbitMQ()) {
            Channel channel = rabbit.getChannel();
            channel.queueDeclare("work", false, false, false, null);
            for (int i = 0; i < 1000; i++) {
                String msg = String.format("index: %s,hello work rabbitmq", i);
                System.out.println("发送消息：" + msg);
                channel.basicPublish("", "work", null, msg.getBytes());
            }
        }
    }

}
