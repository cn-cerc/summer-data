package cn.cerc.db.queue.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.MessageProperties;

import cn.cerc.db.core.Curl;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.maintain.MaintainConfig;
import cn.cerc.db.queue.OnStringMessage;
import cn.cerc.db.queue.entity.CheckMQEntity;
import cn.cerc.mis.exception.TimeoutException;

public class RabbitQueue implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RabbitQueue.class);
    private int maximum = 1;
    private Channel channel;
    private final String queueId;

    public RabbitQueue(String queueId) {
        this.queueId = queueId;
    }

    private void initChannel() {
        Connection connection = null;
        try {
            connection = RabbitServer.getInstance().getConnection();
            this.channel = connection.createChannel();
            if (this.channel == null)
                throw new RuntimeException("rabbitmq channel 创建失败，请立即检查 mq 的服务状态");

            this.channel
                    .addShutdownListener(cause -> log.debug("{} rabbitmq channel closed", channel.getChannelNumber()));
            this.channel.basicQos(this.maximum);
            this.channel.queueDeclare(queueId, true, false, false, null);
        } catch (IOException | InterruptedException e) {
            Curl curl = new Curl();
            ServerConfig config = ServerConfig.getInstance();
            String site = config.getProperty("qc.api.rabbitmq.heartbeat.site");
            if (Utils.isEmpty(site)) {
                log.error("未配置rabbitmq心跳监测地址 qc.api.rabbitmq.heartbeat.site");
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
        } finally {
            if (connection != null)
                RabbitServer.getInstance().releaseConnection(connection);
        }
    }

    /**
     * push 模式使用
     */
    public void watch(OnStringMessage consumer) {
        initChannel();
        if (consumer != null && channel != null) {
            try {
                channel.basicConsume(queueId, false, new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
                            byte[] body) throws IOException {
                        String msg = new String(body);
                        long startTime = System.currentTimeMillis();
                        try {
                            if (channel == null || !channel.isOpen())
                                return;
                            synchronized (channel) {
                                if (channel == null || !channel.isOpen())
                                    return;
                                if (consumer.consume(msg, true))
                                    channel.basicAck(envelope.getDeliveryTag(), false); // 通知服务端删除消息
                                else
                                    channel.basicReject(envelope.getDeliveryTag(), true);// 拒绝本次消息，服务端二次发送
                            }
                        } catch (Exception e) {
                            channel.basicReject(envelope.getDeliveryTag(), true);// 拒绝本次消息，服务端二次发送
                            String message = String.format("queueId %s, payload %s, message %s", queueId, msg,
                                    e.getMessage());
                            log.error(message, e);
                        } finally {
                            long endTime = System.currentTimeMillis() - startTime;
                            if (endTime > TimeoutException.Timeout)
                                log.warn(consumer.getClass().getSimpleName(), msg, endTime);
                        }
                        if (MaintainConfig.build().illegalConsume()) {
                            log.warn("运维正在检修，异常消费 push 消息，队列编号 {}, 消息内容 {}", queueId, msg);
                        }
                    }
                });
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    /**
     * pull 模式使用
     */
    // 读取work队列中的一条消息，ack = false 需要手动确认消息已被读取
    public void pop(OnStringMessage resume) throws IOException {
        initChannel();
        for (int i = 0; i < maximum; i++) {
            GetResponse response;
            try {
                response = channel.basicGet(this.queueId, false);
            } catch (IOException e) {
                log.error("queueId {}, message {}", this.queueId, e.getMessage(), e);
                return;
            }
            if (response == null) {
                return;
            }

            // 手动设置消息已被读取
            String msg = new String(response.getBody());
            Envelope envelope = response.getEnvelope();
            long startTime = System.currentTimeMillis();
            try {
                if (resume.consume(msg, true))
                    channel.basicAck(envelope.getDeliveryTag(), false);// 通知服务端删除消息
                else
                    channel.basicReject(envelope.getDeliveryTag(), true);// 拒绝本次消息，服务端二次发送
                if (MaintainConfig.build().illegalConsume()) {
                    log.warn("运维正在检修，异常消费 pull 消息，队列编号 {}, 消息内容 {}", this.queueId, msg);
                }
            } catch (Exception e) {
                channel.basicReject(envelope.getDeliveryTag(), true);// 拒绝本次消息，服务端二次发送
                String message = String.format("queueId %s, payload %s, message %s", this.queueId, msg, e.getMessage());
                log.error(message, e);
            } finally {
                long endTime = System.currentTimeMillis() - startTime;
                if (endTime > TimeoutException.Timeout)
                    log.warn(resume.getClass().getSimpleName(), msg, endTime);
            }
        }
    }

    /**
     * 生产者发送消息
     */
    public String push(String msg) {
        if (MaintainConfig.build().illegalProduce()) {
            log.warn("运维正在检修，异常生产消息，队列编号 {}, 消息内容 {}", this.queueId, msg);
        }
        initChannel();
        boolean result = false;
        try {
            channel.confirmSelect();
            channel.basicPublish("", this.queueId, MessageProperties.PERSISTENT_TEXT_PLAIN,
                    msg.getBytes(StandardCharsets.UTF_8));
            result = channel.waitForConfirms();
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        if (result) {
            return "ok";
        } else {
            log.error("{} 消息 {} 发送失败", this.getClass().getSimpleName(), msg);
            String error = String.format("%s 消息发送失败", this.getClass().getSimpleName());
            throw new RuntimeException(error);
        }
    }

    public int getMaximum() {
        return maximum;
    }

    public void setMaximum(int maximum) {
        this.maximum = maximum;
    }

    public int getMessageCount() {
        int value = 0;
        try {
            value = this.channel.queueDeclare().getMessageCount();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return value;
    }

    @Override
    public void close() {
        if (channel != null) {
            synchronized (channel) {
                try {
                    channel.close();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
                channel = null;
            }
        }
    }

}
