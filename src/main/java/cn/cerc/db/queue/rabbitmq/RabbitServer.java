package cn.cerc.db.queue.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.zk.ZkNode;

public enum RabbitServer {
    INSTANCE;

    private static final Logger log = LoggerFactory.getLogger(RabbitServer.class);
    private final AtomicInteger atomic = new AtomicInteger();

    private ConnectionFactory factory;
    private Connection connection;

    public static RabbitServer getInstance() {
        return INSTANCE;
    }

    public ConnectionFactory getFactory() {
        if (factory != null)
            return factory;

        synchronized (RabbitServer.class) {
            if (factory == null) {
                final String prefix = String.format("/%s/%s/rabbitmq/", ServerConfig.getAppProduct(),
                        ServerConfig.getAppVersion());
                var host = ZkNode.get().getNodeValue(prefix + "host", () -> "rabbitmq.local.top");
                var port = ZkNode.get().getNodeValue(prefix + "port", () -> "5672");
                var username = ZkNode.get().getNodeValue(prefix + "username", () -> "admin");
                var password = ZkNode.get().getNodeValue(prefix + "password", () -> "admin");

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

    public Connection getConnection() {
        if (connection != null)
            return connection;

        synchronized (RabbitServer.class) {
            if (connection == null) {
                ConnectionFactory factory = RabbitServer.getInstance().getFactory();
                if (this.atomic.get() >= 3) {
                    log.error("{}:{} rabbitmq 尝试连接 {} 次失败，不再进行尝试", factory.getHost(), factory.getPort(),
                            this.atomic.get());
                    return null;
                }

                try {
                    connection = factory.newConnection();
                    if (connection == null)
                        throw new RuntimeException("rabbitmq connection 创建失败，请立即检查 mq 的服务状态");
                    connection.addShutdownListener(cause -> log.debug("{}:{} rabbitmq connection closed",
                            factory.getHost(), factory.getPort()));
                } catch (IOException | TimeoutException e) {
                    if (this.atomic.get() < 3) {
                        log.error("{}:{} {}", factory.getHost(), factory.getPort(), e.getMessage(), e);
                        this.atomic.incrementAndGet();
                    }
                }
            }
        }
        return connection;

    }

    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            connection = null;
        }
    }

}