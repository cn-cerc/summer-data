package cn.cerc.db.queue.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.zk.ZkNode;

public enum RabbitServer {
    INSTANCE;

    private ConnectionFactory factory;

    public ConnectionFactory getFactory() {
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
        return factory;
    }

}