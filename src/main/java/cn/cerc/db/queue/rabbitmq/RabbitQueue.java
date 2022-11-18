package cn.cerc.db.queue.rabbitmq;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

import cn.cerc.db.queue.OnStringMessage;

public class RabbitQueue {
    private static final Logger log = LoggerFactory.getLogger(RabbitQueue.class);
    private Channel channel;
    private String queueId;

    public RabbitQueue(Channel channel, String queueId) {
        this.channel = channel;
        this.queueId = queueId;
        try {
            channel.queueDeclare(queueId, true, false, false, null);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void watch(OnStringMessage resume) {
        try {
            channel.basicConsume(queueId, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
                        byte[] body) throws IOException {
                    String msg = new String(body);
                    resume.consume(msg);
                }
            });
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    // 读取work队列中的一条消息，ack = false 需要手动确认消息已被读取
    public void pop(int maximum, OnStringMessage resume) {
        try {
            for (int i = 0; i < maximum; i++) {
                GetResponse response = channel.basicGet(this.queueId, false);
                if (response == null)
                    return;
                String msg = new String(response.getBody());
                // 手动设置消息已被读取
                if (resume.consume(msg))
                    channel.basicAck(response.getEnvelope().getDeliveryTag(), true);
                else
                    channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public String push(String msg) {
        try {
            channel.basicPublish("", this.queueId, null, msg.getBytes());
            return "ok";
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return "error";
        }
    }

}
