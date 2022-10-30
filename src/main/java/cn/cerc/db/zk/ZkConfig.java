package cn.cerc.db.zk;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.cerc.db.core.Utils;

public class ZkConfig {
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

    public String getString(String key, String def) {
        var result = server.getValue(createKey(path, key));
        return result != null ? result : def;
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

    public static void main(String[] args) {
        // 直接使用
        System.out.println(server.exists("/java"));
        System.out.println(server.exists("/java9"));
        System.out.println(server.getValue("/java"));
        //
        ZkConfig conf = new ZkConfig("/mysql");
        System.out.println(conf.exists());
        // 赋值
        conf.setValue("host", "127.0.01");
        conf.setValue("port", "80");
        // 取值
        System.out.println(conf.getString("port", ""));
        // 列出所有的子key
        for (var item : conf.list()) {
            System.out.println(item);
        }
        // 列出所有的子key与值
        conf.map().forEach((key, value) -> {
            System.out.println(String.format("key: %s, value=%s", key, value));
        });
        server.close();
    }

}
