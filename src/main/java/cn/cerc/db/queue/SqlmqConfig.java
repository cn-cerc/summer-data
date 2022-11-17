package cn.cerc.db.queue;

import cn.cerc.db.mysql.MysqlConfigImpl;
import cn.cerc.db.zk.ZkNode;

public class SqlmqConfig implements MysqlConfigImpl {

    @Override
    public String getHost() {
        return ZkNode.get().getString(getKey("host"), "127.0.0.1");
    }

    @Override
    public String getUsername() {
        return ZkNode.get().getString(getKey("username"), "");
    }

    @Override
    public String getPassword() {
        return ZkNode.get().getString(getKey("password"), "");
    }

    @Override
    public String getDatabase() {
        return ZkNode.get().getString(getKey("database"), "sqlmq");
    }

    private String getKey(String key) {
        return "sqlmq/" + key;
    }

}
