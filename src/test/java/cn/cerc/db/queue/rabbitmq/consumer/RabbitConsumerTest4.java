package cn.cerc.db.queue.rabbitmq.consumer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import cn.cerc.db.queue.rabbitmq.config.RabbitClient;

public class RabbitConsumerTest4 {
    private static final Logger log = LoggerFactory.getLogger(RabbitConsumerTest4.class);

    private static final int MAX_THREAD_SIZE = Runtime.getRuntime().availableProcessors();
    private static final ExecutorService pool = Executors.newFixedThreadPool(MAX_THREAD_SIZE);
    private final static String QUEUE_NAME = "queue_work";

    public static void main(String[] args) {
        for (int i = 0; i < 128; i++) {
            pool.submit(() -> {
                try {
                    // 获取到连接
                    Connection connection = RabbitClient.getConnection();
                    connection.addShutdownListener(
                            cause -> log.info("{} Connection Closed.", Thread.currentThread().getName()));
                    // 获取通道
                    Channel channel = connection.createChannel();
                    channel.addShutdownListener(cause -> log.info("{} Channel {} Closed.",
                            Thread.currentThread().getName(), channel.getChannelNumber()));

                    // 消费者预取的消费数量
                    channel.basicQos(100);
                    // 声明队列
                    channel.queueDeclare(QUEUE_NAME, true, false, false, null);

                    // 创建消费者监听器
                    DefaultConsumer consumer = new DefaultConsumer(channel) {
                        /**
                         * consumerTag 同一个会话， consumerTag 是固定的 可以做此会话的名字 envelope 可以通过该对象获取当前消息的编号 发送的队列
                         * 交换机信息 properties 随消息一起发送的其他属性 body 消息内容
                         */
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
                                byte[] body) throws IOException {
                            String mess = new String(body);
//                System.out.println("1接收到的消息：" + mess);
                            // 手动返回一个回执确认
                            /**
                             * 1.要回执确认的消息的编号 2.是否批量确认 true:批量确认 false:只确认当前消息
                             */
                            channel.basicAck(envelope.getDeliveryTag(), false);
                        }
                    };
                    // 监听队列，false表示手动返回完成状态，true表示自动
                    channel.basicConsume(QUEUE_NAME, false, consumer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

}