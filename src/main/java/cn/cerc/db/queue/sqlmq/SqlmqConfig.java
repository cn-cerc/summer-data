package cn.cerc.db.queue.sqlmq;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.mysql.MysqlConfigImpl;
import cn.cerc.db.zk.ZkNode;

public class SqlmqConfig implements MysqlConfigImpl {

    private final String prefix = String.format("/%s/%s", ServerConfig.getAppProduct(), ServerConfig.getAppVersion());

    @Override
    public String getHost() {
        return ZkNode.get().getNodeValue(getKey("host"), () -> "sqlmq.local.top");
    }

    @Override
    public String getUsername() {
        return ZkNode.get().getNodeValue(getKey("username"), () -> "sqlmq_user");
    }

    @Override
    public String getPassword() {
        return ZkNode.get().getNodeValue(getKey("password"), () -> "sqlmq_password");
    }

    @Override
    public String getDatabase() {
        return ZkNode.get().getNodeValue(getKey("database"), () -> "sqlmq");
    }

    private String getKey(String key) {
        return this.prefix + "/sqlmq/" + key;
    }

}
