package cn.cerc.db.queue.rabbitmq;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * rabbitMQ 连接管理
 */
public class RabbitServer {
    private static final Logger log = LoggerFactory.getLogger(RabbitServer.class);
    private static final int capacity = Runtime.getRuntime().availableProcessors();
    private static final BlockingQueue<Connection> connections = new ArrayBlockingQueue<>(capacity);
    private static final RabbitServer instance = new RabbitServer();
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);

    public static RabbitServer getInstance() {
        return instance;
    }

    /**
     * RabbitServer初始化时存在一个已知bug
     * <p>
     * 当容器启动时
     * <p>
     * 线程一：触发 log.warn 或 log.error 进入 JayunLogAppender 方法， 线程一等待 RabbitServer
     * 初始化，线程一进行阻塞
     * <p>
     * 主线程：对 RabbitServer 进行初始化执行到 log 输出时由于 org.apache.log4j.Category.callAppenders
     * 方法存在同步代码块需要等待线程一的 JayunLogAppender 执行完成
     * <p>
     * 由此造成死锁，导致容器启动卡住
     */
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
//                log.debug("初始化连接 {}", i);
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
//        log.debug("初始化完成 {}", capacity);
    }

    /**
     * 获取连接，自动重连
     * <p>
     * 从连接池中获取连接，如果池为空则阻塞等待
     *
     * @return connection
     */
    public Connection getConnection() throws InterruptedException {
        log.debug("准备取出，剩余个数 {}", connections.size());
        Connection connection = connections.take();
        log.debug("取出连接，剩余个数 {}", connections.size());
//        return connections.poll(5, TimeUnit.SECONDS);
        return connection;
    }

    /**
     * 将连接放回连接池
     */
    public void releaseConnection(Connection connection) {
        if (shutdown.get()) {
            log.info("rabbitmq 线程池已关闭，不再接收连接归还");
            try {
                connection.close();
            } catch (IOException e) {
                log.error("rabbitmq 归还时关闭异常 {}", e.getMessage(), e);
            }
            return;
        }
        connections.add(connection);
        log.debug("归还连接，剩余个数 {}", connections.size());
    }

    /**
     * 关闭所有连接
     */
    public void close() throws IOException {
        log.debug("关闭连接，剩余个数 {}", connections.size());
        for (Connection connection : connections) {
            connection.close();
        }
        shutdown.set(true);
    }

}