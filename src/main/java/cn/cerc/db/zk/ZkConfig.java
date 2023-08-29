package cn.cerc.db.zk;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;

public class ZkConfig implements IConfig {
//    private static final Logger log = LoggerFactory.getLogger(ZkConfig.class);
    private ZkServer server;
    private String path;

    public ZkConfig(String path) {
        super();
        if (Utils.isEmpty(path))
            throw new RuntimeException("path 不允许为空");
        if (path.endsWith("/"))
            throw new RuntimeException("path 不得以 / 结尾");

        this.path = String.format("/%s/%s/%s%s", ServerConfig.getAppProduct(), ServerConfig.getAppVersion(),
                ServerConfig.getAppOriginal(), path);

        server = ZkNode.get().server();
    }

    public String path() {
        return this.path;
    }

    public String path(String key) {
        return createKey(this.path, key);
    }

    public static String createKey(String path, String key) {
        if (path == null || "".equals(path))
            path = "/";
        String result;
        if (path.endsWith("/"))
            result = path + key;
        else
            result = path + "/" + key;
        return result;
    }

    @Override
    public String getProperty(String key, String def) {
        String result = ZkNode.get().getNodeValue(path(key), () -> def);
        return Utils.isEmpty(result) ? def : result;
    }

    public String getString(String key, String def) {
        return this.getProperty(key, def);
    }

    public String getString(String key) {
        return this.getProperty(key, "");
    }

    public int getInt(String key, int def) {
        var result = this.getProperty(key, "" + def);
        return Integer.parseInt(result);
    }

    public int getInt(String key) {
        var result = this.getProperty(key, "0");
        return Integer.parseInt(result);
    }

    public void setValue(String key, String value) {
        server.setValue(path(key), value == null ? "" : value, CreateMode.PERSISTENT);
    }

    public void setTempNode(String key, String value) {
        server.setValue(path(key), value == null ? "" : value, CreateMode.EPHEMERAL);
    }

    public List<String> list() {
        return server.getNodes(path);
    }

    public Map<String, String> map() {
        Map<String, String> result = new HashMap<>();
        for (var key : this.list()) {
            result.put(key, this.getString(key, null));
        }
        return result;
    }

    public boolean exists() {
        return server.exists(path);
    }

    public boolean exists(String key) {
        return server.exists(path(key));
    }

    public void delete(String key) {
        server.delete(path(key));
    }

    public ZooKeeper client() {
        return server.client();
    }

    @Deprecated
    public ZkServer server() {
        return ZkNode.get().server();
    }
}
