package cn.cerc.db.queue.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RabbitServer.class);
    private Connection connection;
    private Channel channel;
    private static RabbitServer instance;

    // 创建连接
    public RabbitServer() {
        try {
            RabbitConfig config = new RabbitConfig();
            // 创建连接工厂
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(config.getHost());
            connectionFactory.setPort(config.getPort());
            // 设置连接哪个虚拟主机
//            connectionFactory.setVirtualHost("/test-1");
            connectionFactory.setUsername(config.getUsername());
            connectionFactory.setPassword(config.getPassword());
            this.connection = connectionFactory.newConnection();
            this.channel = connection.createChannel();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public synchronized static RabbitQueue getQueue(String queueId) {
        if (instance == null)
            instance = new RabbitServer();
        return new RabbitQueue(instance.channel, queueId);
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

}