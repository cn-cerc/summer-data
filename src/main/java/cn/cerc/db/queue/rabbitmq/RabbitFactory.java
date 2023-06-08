package cn.cerc.db.queue.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.zk.ZkNode;

/**
 * rabbit 连接工厂类
 */
public enum RabbitFactory {

    instance;

    private static final Logger log = LoggerFactory.getLogger(RabbitFactory.class);
    private ConnectionFactory factory;

    public static RabbitFactory getInstance() {
        return instance;
    }

    public ConnectionFactory getFactory() {
        if (factory != null)
            return factory;

        synchronized (RabbitFactory.class) {
            if (factory == null) {
                final String prefix = String.format("/%s/%s/rabbitmq/", ServerConfig.getAppProduct(),
                        ServerConfig.getAppVersion());
                String host = ZkNode.get().getNodeValue(prefix + "host", () -> "rabbitmq.local.top");
                String port = ZkNode.get().getNodeValue(prefix + "port", () -> "5672");
                String username = ZkNode.get().getNodeValue(prefix + "username", () -> "admin");
                String password = ZkNode.get().getNodeValue(prefix + "password", () -> "admin");

                // 创建连接工厂
                factory = new ConnectionFactory();
                factory.setHost(host);
                factory.setPort(Integer.parseInt(port));
                factory.setUsername(username);
                factory.setPassword(password);
                factory.setConnectionTimeout(30000);
                factory.setRequestedHeartbeat(60);
            }
        }
        return factory;
    }

    public Connection newConnection() throws IOException, TimeoutException {
        Connection connection = RabbitFactory.getInstance().getFactory().newConnection();
        if (connection == null)
            throw new RuntimeException("rabbitmq connection 创建失败，请立即检查 RabbitMQ 的服务状态");
        connection.addShutdownListener(
                cause -> log.info("{}:{} rabbitmq connection closed", factory.getHost(), factory.getPort()));
        return connection;
    }

}