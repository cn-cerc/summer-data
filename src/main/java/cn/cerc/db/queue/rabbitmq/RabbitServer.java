package cn.cerc.db.queue.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;

public class RabbitServer {
    private static final Logger log = LoggerFactory.getLogger(RabbitServer.class);

    private static final RabbitServer instance = new RabbitServer();
    private Connection connection;

    public static RabbitServer getInstance() {
        return instance;
    }

    private RabbitServer() {
    }

    /**
     * 获取连接，自动重连
     * 
     * @return connection
     */
    public Connection getConnection() {
        if (this.connection != null) {
            return this.connection;
        }
        synchronized (RabbitServer.class) {
            if (this.connection == null) {
                try {
                    this.connection = RabbitFactory.getInstance().newConnection();
                    log.info("创建了新的 rabbitmq 连接");
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        }
        return this.connection;
    }

    /**
     * 获取连接，自动重连
     * 
     * @return connection
     */
    public Connection reconnec() {
        this.connection = null;
        synchronized (RabbitServer.class) {
            if (this.connection == null) {
                try {
                    this.connection = RabbitFactory.getInstance().newConnection();
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        }
        return this.connection;
    }

}