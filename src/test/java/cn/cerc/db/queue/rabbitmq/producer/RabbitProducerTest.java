package cn.cerc.db.queue.rabbitmq.producer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import cn.cerc.db.queue.rabbitmq.config.RabbitClient;

public class RabbitProducerTest {
    private static final AtomicLong atomic = new AtomicLong();
    private final static String QUEUE_NAME = "queue_work";

    public static void main(String[] args) {
        try {
            // 获取到连接
            Connection connection = RabbitClient.getConnection();
            // 从连接中创建通道
            Channel channel = connection.createChannel();
            // 声明队列，如果队列已经存在，则使用这个队列
            /**
             * 1.队列名字
             * 
             * 2.是否持久化 true:持久话 false:非持久话
             * 
             * 3.是否独占模式 true:独占模式 false:非独占
             * 
             * 4.是否自动删除队列中的消息 true:断开连接删除消息 false:断开连接不会删除消息
             * 
             * 5.其他额外参数
             */
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            for (int j = 1; j <= 100; j++) {
                // 消息内容
                String message = String.join("-", Thread.currentThread().getName(),
                        String.valueOf(atomic.incrementAndGet()));
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
                System.out.println("发送: " + message);
            }
            channel.close();
            connection.close();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

}
