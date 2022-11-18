package cn.cerc.db.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQ implements AutoCloseable {

    public static final String host = "110.41.140.86";
    public static final int port = 5672;
    public static final String user = "admin";
    public static final String pass = "admin";

    private Connection connection;
    private Channel channel;

    // 创建连接
    public RabbitMQ() {
        try {
            // 创建连接工厂
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(host);
            connectionFactory.setPort(port);
            // 设置连接哪个虚拟主机
//            connectionFactory.setVirtualHost("/test-1");
            connectionFactory.setUsername(user);
            connectionFactory.setPassword(pass);
            this.connection = connectionFactory.newConnection();
            this.channel = connection.createChannel();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    @Override
    public void close() {
        try {
            if (channel != null) {
                channel.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}