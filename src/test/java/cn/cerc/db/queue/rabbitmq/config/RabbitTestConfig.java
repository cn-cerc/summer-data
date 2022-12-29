package cn.cerc.db.queue.rabbitmq.config;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitTestConfig {
    public final static String QUEUE_NAME = "queue_work";
    public final static String EXCHANGE_DIRECT_DTIENG = "diteng.direct";

    private static final ConnectionFactory factory = new ConnectionFactory();

    static {
        factory.setHost("127.0.0.1");
//        factory.setHost("172.16.0.212"); // itjun-buntu
//        factory.setHost("121.37.154.92"); // 4plc-huawei-jenkins
        factory.setPort(5672); // 代理服务器端口
        factory.setUsername("admin");
        factory.setPassword("admin");
//        connectionFactory.setVirtualHost("testHA"); // 虚拟主机
    }

    public static Connection getConnection() {
        try {
            Connection connection = factory.newConnection();
            return connection;
        } catch (IOException | TimeoutException e) {
            return null;
        }
    }

}
