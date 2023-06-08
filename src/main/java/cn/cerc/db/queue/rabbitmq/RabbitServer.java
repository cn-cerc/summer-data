package cn.cerc.db.queue.rabbitmq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;

public class RabbitServer {
    private static final Logger log = LoggerFactory.getLogger(RabbitServer.class);
    private static final int poolSize = Runtime.getRuntime().availableProcessors();
    private static final List<Connection> connections = new ArrayList<>(poolSize);
    private static final AtomicInteger loader = new AtomicInteger();

    private static final RabbitServer instance = new RabbitServer();

    public static RabbitServer getInstance() {
        return instance;
    }

    private RabbitServer() {
        for (int i = 0; i < poolSize; i++) {
            try {
                Connection connection = RabbitFactory.getInstance().newConnection();
                connections.add(connection);
            } catch (IOException | TimeoutException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 获取连接，自动重连
     * 
     * @return connection
     */
    public Connection getConnection() {
        return connections.get(next());
    }

    /**
     * 获取下一个连接游标
     * 
     * @return index 负载游标
     */
    private int next() {
        int index = loader.getAndIncrement();
        if (loader.get() > poolSize) {
            loader.set(0);
            index = 0;
        }
        return index;
    }

    /**
     * 关闭连接
     */
    public void close() {
        for (Connection connection : connections) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        log.info("RabbitMQ 连接已关闭");
    }

}