package cn.cerc.db.redis;

import cn.cerc.db.core.Utils;
import cn.cerc.db.zk.ZkNode;

public class ZkRedisConfig {
    private final String configId;
    private final ZkNode node;

    public ZkRedisConfig() {
        this(null);
    }

    public ZkRedisConfig(String configId) {
        this.configId = configId;
        this.node = ZkNode.get();
    }

    public String host() {
        return node.getString(key("host"), "127.0.0.1");
    }

    public int port() {
        return node.getInt(key("port"), 6379);
    }

    public String password() {
        String value = node.getString(key("password"), "");
        if (Utils.isEmpty(value))
            return null;
        else
            return value;
    }

    public int timeout() {
        return node.getInt(key("timeout"), 10000);
    }

    public String getFullPath() {
        if (Utils.isEmpty(this.configId))
            return String.format("%s/redis", node.rootPath());
        else
            return String.format("%s/redis-%s", configId, ZkNode.get(), this.configId);
    }

    private String key(String key) {
        if (Utils.isEmpty(this.configId))
            return String.format("redis/%s", key);
        else
            return String.format("redis-%s/%s", configId, key);
    }

}
