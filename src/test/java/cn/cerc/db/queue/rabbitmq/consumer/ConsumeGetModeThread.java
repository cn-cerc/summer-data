package cn.cerc.db.queue.rabbitmq.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;

import cn.cerc.db.queue.rabbitmq.config.RabbitTestConfig;

/**
 * 使用Get模式读取MQ中的数据
 */
public class ConsumeGetModeThread {

    private static final Logger log = LoggerFactory.getLogger(ConsumeGetModeThread.class);

    public static void main(String[] args) {
        try {
            // 获取到连接
            Connection connection = RabbitTestConfig.getConnection();
            connection
                    .addShutdownListener(cause -> log.info("{} Connection Closed.", Thread.currentThread().getName()));
            // 获取通道
            Channel channel = connection.createChannel();
            channel.addShutdownListener(cause -> log.info("{} Channel {} Closed.", Thread.currentThread().getName(),
                    channel.getChannelNumber()));

            // 消费者预取的消费数量
            channel.basicQos(1);
            channel.queueDeclare(RabbitTestConfig.QUEUE_NAME, true, false, false, null);

            // 声明路由
            channel.exchangeDeclare(RabbitTestConfig.EXCHANGE_DIRECT_DTIENG, BuiltinExchangeType.DIRECT, true, false,
                    false, null);
            // 队列绑定
            channel.queueBind(RabbitTestConfig.QUEUE_NAME, RabbitTestConfig.EXCHANGE_DIRECT_DTIENG,
                    RabbitTestConfig.QUEUE_NAME);

            GetResponse response = channel.basicGet(RabbitTestConfig.QUEUE_NAME, false);
            System.out.println(new String(response.getBody()));
            channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

}
