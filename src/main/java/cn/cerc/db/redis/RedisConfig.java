package cn.cerc.db.redis;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.zk.ZkNode;

public class RedisConfig {
    private final String configId;
    private final ZkNode node;

    private final String prefix = String.format("/%s/%s", ServerConfig.getAppProduct(), ServerConfig.getAppVersion());

    public RedisConfig() {
        this(null);
    }

    public RedisConfig(String configId) {
        this.configId = configId;
        this.node = ZkNode.get();
    }

    public String host() {
        return node.getNodeValue(key("host"), () -> "127.0.0.1");
    }

    public int port() {
        return Integer.parseInt(node.getNodeValue(key("port"), () -> "6379"));
    }

    public String password() {
        return node.getNodeValue(key("password"), () -> "");
    }

    public int timeout() {
        return Integer.parseInt(node.getNodeValue(key("timeout"), () -> "10000"));
    }

    public String getFullPath() {
        if (Utils.isEmpty(this.configId))
            return String.format("%s/redis", this.prefix);
        else
            return String.format("%s/redis-%s", this.prefix, this.configId);
    }

    private String key(String key) {
        if (Utils.isEmpty(this.configId))
            return String.format("%s/redis/%s", this.prefix, key);
        else
            return String.format("%s/redis-%s/%s", this.prefix, this.configId, key);
    }

}
