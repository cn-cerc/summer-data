package cn.cerc.db.mysql;

import cn.cerc.db.zk.ZkNode;

public class ZkMysqlConfig {

    private ZkNode node = ZkNode.get();

    public String site() {
        return node.getString(getNodePath("site"), "127.0.0.1:3306");
    }

    public String database() {
        return node.getString(getNodePath("database"), "appdb");
    }

    public String username() {
        return node.getString(getNodePath("username"), "appdb_user");
    }

    public String password() {
        return node.getString(getNodePath("password"), "appdb_password");
    }

    public String serverTimezone() {
        return node.getString(getNodePath("serverTimezone"), "Asia/Shanghai");
    }

    public int maxPoolSize() {
        return node.getInt(getNodePath("MaxPoolSize"), 0);
    }

    public int minPoolSize() {
        return node.getInt(getNodePath("MinPoolSize"), 9);
    }

    public int initialPoolSize() {
        return node.getInt(getNodePath("InitialPoolSize"), 3);
    }

    public int checkoutTimeout() {
        return node.getInt(getNodePath("CheckoutTimeout"), 500);
    }

    public int maxIdleTime() {
        return node.getInt(getNodePath("MaxIdleTime"), 7800);
    }

    public int idleConnectionTestPeriod() {
        return node.getInt(getNodePath("IdleConnectionTestPeriod"), 9);
    }

    private String getNodePath(String key) {
        return String.format("%s/%s", "mysql", key);
    }

}
