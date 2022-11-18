package cn.cerc.db.queue.rabbitmq;

import cn.cerc.db.zk.ZkNode;

public class RabbitConfig {
    private static final String Root = "RabbitMQ";

    public String getHost() {
        return ZkNode.get().getString(Root + "/host", "127.0.0.1");
    }

    public int getPort() {
        return ZkNode.get().getInt(Root + "/port", 5672);
    }

    public String getUsername() {
        return ZkNode.get().getString(Root + "/username", "");
    }

    public String getPassword() {
        return ZkNode.get().getString(Root + "/password", "");
    }

}
