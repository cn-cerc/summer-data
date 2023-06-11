package cn.cerc.db.queue.rabbitmq;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * rabbitMQ 连接管理
 *
 */
public class RabbitServer {
    private static final Logger log = LoggerFactory.getLogger(RabbitServer.class);
    private static final int capacity = Runtime.getRuntime().availableProcessors();
    private static final BlockingQueue<Connection> connections = new ArrayBlockingQueue<>(capacity);
    private static final RabbitServer instance = new RabbitServer();

    public static RabbitServer getInstance() {
        return instance;
    }

    private RabbitServer() {
        ConnectionFactory factory = RabbitFactory.getInstance().build();
        for (int i = 0; i < capacity; i++) {
            try {
                Connection connection = factory.newConnection();
                if (connection == null)
                    throw new RuntimeException("rabbitmq connection 创建失败，请立即检查 RabbitMQ 的服务状态");
                connection.addShutdownListener(
                        cause -> log.info("{}:{} rabbitmq connection closed", factory.getHost(), factory.getPort()));
                connections.add(connection);
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    /**
     * 获取连接，自动重连
     *
     * 从连接池中获取连接，如果池为空则阻塞等待
     * 
     * @return connection
     * @throws InterruptedException
     */
    public Connection getConnection() throws InterruptedException {
        return connections.take();
    }

    /**
     * 将连接放回连接池
     * 
     * @param connection
     */
    public void releaseConnection(Connection connection) {
        connections.add(connection);
    }

    /**
     * 关闭所有连接
     */
    public void close() throws IOException {
        for (Connection connection : connections) {
            connection.close();
        }
    }

}