package cn.cerc.db.queue.sqlmq;

import cn.cerc.db.mysql.MysqlConfigImpl;
import cn.cerc.db.zk.ZkNode;

public class SqlmqConfig implements MysqlConfigImpl {

    @Override
    public String getHost() {
        return ZkNode.get().getString(getKey("host"), "sqlmq.local.top");
    }

    @Override
    public String getUsername() {
        return ZkNode.get().getString(getKey("username"), "sqlmq_user");
    }

    @Override
    public String getPassword() {
        return ZkNode.get().getString(getKey("password"), "sqlmq_password");
    }

    @Override
    public String getDatabase() {
        return ZkNode.get().getString(getKey("database"), "sqlmq");
    }

    private String getKey(String key) {
        return "sqlmq/" + key;
    }

}
