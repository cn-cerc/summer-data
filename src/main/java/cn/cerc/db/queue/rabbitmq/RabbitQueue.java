package cn.cerc.db.queue.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.MessageProperties;

import cn.cerc.db.core.Curl;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.queue.OnStringMessage;
import cn.cerc.db.queue.entity.CheckMQEntity;

public class RabbitQueue implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RabbitQueue.class);
    private int maximum = 1;
    private Channel channel;
    private Connection connection;
    private final String queueId;

    public RabbitQueue(String queueId) {
        this.queueId = queueId;
    }

    private void initChannel() {
        ConnectionFactory factory = RabbitServer.INSTANCE.getFactory();
        try {
            connection = factory.newConnection();
            if (connection == null)
                throw new RuntimeException("rabbitmq connection 创建失败，请立即检查 mq 的服务状态");
            connection.addShutdownListener(
                    cause -> log.debug("{}:{} rabbitmq connection closed", factory.getHost(), factory.getPort()));

            channel = connection.createChannel();
            if (channel == null)
                throw new RuntimeException("rabbitmq channel 创建失败，请立即检查 mq 的服务状态");

            channel.addShutdownListener(cause -> log.debug("{} rabbitmq channel closed", channel.getChannelNumber()));
            channel.basicQos(this.maximum);
            channel.queueDeclare(queueId, true, false, false, null);
        } catch (IOException | TimeoutException e) {
            log.error("{}:{} {}", factory.getHost(), factory.getPort(), e.getMessage(), e);
            Curl curl = new Curl();
            ServerConfig config = ServerConfig.getInstance();
            String site = config.getProperty("qc.api.rabbitmq.heartbeat.site");
            if (Utils.isEmpty(site)) {
                log.error("未配置rabbitmq心跳监测地址");
                return;
            }
            String project = ServerConfig.getAppProduct();
            String version = ServerConfig.getAppVersion();
            CheckMQEntity entity = new CheckMQEntity();
            entity.setProjcet(project);
            entity.setVersion(version);
            entity.setAlive(false);
            try {
                curl.doPost(site, entity);
            } catch (Exception ex) {
                log.warn("{} {} MQ连接超时，qc监控MQ接口异常", project, version, ex);
            }
        }
    }

    /**
     * push 模式使用
     */
    public void watch(OnStringMessage consumer) {
        initChannel();
        try {
            if (consumer != null && channel != null) {
                channel.basicConsume(queueId, false, new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
                            byte[] body) throws IOException {
                        String msg = new String(body);
                        try {
                            if (consumer.consume(msg, true))
                                channel.basicAck(envelope.getDeliveryTag(), false); // 通知服务端删除消息
                            else
                                channel.basicReject(envelope.getDeliveryTag(), true);// 拒绝本次消息，服务端二次发送
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                            channel.basicReject(envelope.getDeliveryTag(), true);// 拒绝本次消息，服务端二次发送
                        }
                    }
                });
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * pull 模式使用
     */
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

            // 手动设置消息已被读取
            String msg = new String(response.getBody());
            Envelope envelope = response.getEnvelope();
            try {
                if (resume.consume(msg, true))
                    channel.basicAck(envelope.getDeliveryTag(), false);// 通知服务端删除消息
                else
                    channel.basicReject(envelope.getDeliveryTag(), true);// 拒绝本次消息，服务端二次发送
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                try {
                    channel.basicReject(envelope.getDeliveryTag(), true);// 拒绝本次消息，服务端二次发送
                } catch (IOException e1) {
                    log.error(e1.getMessage(), e1);
                }
            }
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
            log.error("{} 消息 {} 发送失败", this.getClass().getSimpleName(), msg);
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
            if (connection != null) {
                connection.close();
                connection = null;
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
