package cn.cerc.db.zk;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.Utils;

public class ZkConfig implements IConfig {
    private String path;
    private static ZkServer server;

    public ZkConfig(String path) {
        super();
        if (Utils.isEmpty(path))
            throw new RuntimeException("path not is empty");
        this.path = path;
        synchronized (ZkConfig.class) {
            if (ZkConfig.server == null)
                ZkConfig.server = new ZkServer();
        }
    }

    public String path() {
        return this.path;
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
        var result = server.getValue(createKey(path, key));
        return result != null ? result : def;
    }

    public String getString(String key, String def) {
        return this.getProperty(key, def);
    }

    public int getInt(String key, int def) {
        var result = server.getValue(createKey(path, key));
        return result != null ? Integer.parseInt(result) : def;
    }

    public void setValue(String key, String value) {
        server.setValue(createKey(path, key), value);
    }

    public List<String> list() {
        if ("".equals(path))
            return server.getNodes("/");
        else
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
        return server.exists(createKey(path, ""));
    }

}
