package cn.cerc.db.rabbitmq;

import java.io.IOException;

import org.junit.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;

public class PullTest {

    @Test
    public void test() throws IOException, InterruptedException {
        try (RabbitMQ rabbit = new RabbitMQ()) {
            Channel channel = rabbit.getChannel();
            channel.queueDeclare("work", false, false, false, null);
            for (int i = 0; i < 100; i++) {
                // 读取work队列中的一条消息，ack = false 需要手动确认消息已被读取
                GetResponse response = channel.basicGet("work", false);
                String msg = new String(response.getBody());
                System.out.println(msg);
                // 手动设置消息已被读取
                channel.basicAck(response.getEnvelope().getDeliveryTag(), true);
            }
        } catch (Exception e) {
        }
    }

}
