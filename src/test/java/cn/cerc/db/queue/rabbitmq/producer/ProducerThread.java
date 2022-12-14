package cn.cerc.db.queue.rabbitmq.producer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

import cn.cerc.db.queue.rabbitmq.config.RabbitTestConfig;

public class ProducerThread implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ProducerThread.class);

    private static final AtomicLong atomic = new AtomicLong();

    @Override
    public void run() {
        try {
            // 获取连接
            Connection connection = RabbitTestConfig.getConnection();
            // 创建信道
            Channel channel = connection.createChannel();
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
            // 声明队列，如果队列已经存在，则使用这个队列
            channel.queueDeclare(RabbitTestConfig.QUEUE_NAME, true, false, false, null);

            // 声明路由
            channel.exchangeDeclare(RabbitTestConfig.EXCHANGE_DIRECT_DTIENG, BuiltinExchangeType.DIRECT, true, false,
                    false, null);
            // 队列绑定
            channel.queueBind(RabbitTestConfig.QUEUE_NAME, RabbitTestConfig.EXCHANGE_DIRECT_DTIENG,
                    RabbitTestConfig.QUEUE_NAME);

            // 将信道设置为 publisher confirm 模式
            channel.confirmSelect();
            for (int i = 1; i <= 100; i++) {
                // 消息内容
                String message = String.join("-", Thread.currentThread().getName(),
                        String.valueOf(atomic.incrementAndGet()));

                // 推送消息
                channel.basicPublish(RabbitTestConfig.EXCHANGE_DIRECT_DTIENG, RabbitTestConfig.QUEUE_NAME,
                        MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes(StandardCharsets.UTF_8));
//                System.out.println("发送: " + message);
                if (!channel.waitForConfirms())
                    log.error("{} 消息发送失败 {}", Thread.currentThread().getName(), message);
            }
            channel.close();
            connection.close();
            log.info("{} 线程发送完成，当前原子数值 {}", Thread.currentThread().getName(), atomic.get());
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
