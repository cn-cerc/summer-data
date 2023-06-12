package cn.cerc.db.queue.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.zk.ZkNode;

/**
 * rabbit 连接工厂类
 */
public enum RabbitFactory {

    instance;

    private ConnectionFactory factory;

    public static RabbitFactory getInstance() {
        return instance;
    }

    public ConnectionFactory build() {
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

}