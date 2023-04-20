package cn.cerc.db.queue.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.MessageProperties;

import cn.cerc.db.queue.OnStringMessage;

public class RabbitQueue implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RabbitQueue.class);
    private String consumerTag = null;
    private int maximum;
    private Channel channel;
    private String queueId;

    public RabbitQueue(String queueId) {
        this.queueId = queueId;
    }

    private void initChannel() {
        if (channel == null) {
            try {
                channel = RabbitServer.get().getConnection().createChannel();
                channel.addShutdownListener(
                        cause -> log.debug("RabbitMQ channel {} closed.", channel.getChannelNumber()));
                channel.basicQos(this.maximum);
                channel.queueDeclare(queueId, true, false, false, null);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private final ExecutorService pool = Executors.newCachedThreadPool();

    public static record QueueConsumeThread(Channel channel, Envelope envelope, OnStringMessage consumer, byte[] body)
            implements Runnable {
        @Override
        public void run() {
            String msg = new String(body);
            try {
                if (consumer.consume(msg, true))
                    channel.basicAck(envelope.getDeliveryTag(), false);
                else
                    channel.basicReject(envelope.getDeliveryTag(), true);// 拒绝本次消息，服务端二次发送
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void watch(OnStringMessage consumer) {
        initChannel();
        try {
            if (consumer != null) {
                consumerTag = channel.basicConsume(queueId, false, new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
                            byte[] body) throws IOException {
                        QueueConsumeThread consume = new QueueConsumeThread(channel, envelope, consumer, body);
                        if (maximum > 1)
                            pool.submit(consume);
                        else
                            consume.run();
                    }
                });
            } else if (consumerTag != null) {
                channel.basicCancel(consumerTag);
                consumerTag = null;
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    // 读取work队列中的一条消息，ack = false 需要手动确认消息已被读取
    public void pop(OnStringMessage resume) {
        initChannel();
        for (int i = 0; i < maximum; i++) {
            GetResponse response = null;
            try {
                response = channel.basicGet(this.queueId, false);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                return;
            }
            if (response == null)
                return;

            Envelope envelope = response.getEnvelope();
            pool.submit(new QueueConsumeThread(channel, envelope, resume, response.getBody()));
        }
    }

    public String push(String msg) {
        initChannel();
        var result = false;
        try {
            channel.confirmSelect();
            channel.basicPublish("", this.queueId, MessageProperties.PERSISTENT_TEXT_PLAIN,
                    msg.getBytes(StandardCharsets.UTF_8));
            result = channel.waitForConfirms();
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        if (result)
            return "ok";
        else {
            log.error("{} 消息发送失败 {}", this.getClass().getSimpleName(), msg);
            String error = String.format("%s 消息发送失败", this.getClass().getSimpleName());
            throw new RuntimeException(error);
        }
    }

    @Override
    public void close() {
        try {
            if (channel != null) {
                channel.close();
                channel = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int getMaximum() {
        return maximum;
    }

    public void setMaximum(int maximum) {
        this.maximum = maximum;
    }

}
