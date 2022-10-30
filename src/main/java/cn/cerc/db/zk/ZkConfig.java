package cn.cerc.db.zk;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.queue.QueueServer;

public class ZkConfig implements IConfig {
    private static final Logger log = LoggerFactory.getLogger(ZkConfig.class);
    private static final IConfig config = ServerConfig.getInstance();
    private static ZkServer server;
    private String path;

    public ZkConfig(String path) {
        super();
        if (Utils.isEmpty(path))
            throw new RuntimeException("path 不允许为空");
        if (path.endsWith("/"))
            throw new RuntimeException("path 不得以 / 结尾");
        this.path = path;
        synchronized (ZkConfig.class) {
            if (ZkConfig.server == null)
                ZkConfig.server = new ZkServer();
        }
        if ("/redis".equals(path) && !this.exists())
            this.fixRedis();
        if ("/rocketMQ".equals(path) && !this.exists())
            this.fixRocketMQ();
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
        return server.exists(path);
    }

    /**
     * 用于结转旧的配置文件
     * 
     * @param config
     */
    private void fixRedis() {
        log.warn("fixRedis: 自动结转旧的配置数据");
        setValue("host", config.getProperty("redis.host", "127.0.0.1"));
        setValue("port", config.getProperty("redis.port", "6379"));
        setValue("password", config.getProperty("redis.password", ""));
        setValue("timeout", config.getProperty("redis.timeout", "10000"));
    }

    /**
     * 用于结转旧的配置文件
     * 
     * @param config
     */
    private void fixRocketMQ() {
        log.warn("fixRocketMQ: 自动结转旧的配置数据");
        //
        setValue(QueueServer.AliyunAccessKeyId, config.getProperty("mns.accesskeyid", ""));
        setValue(QueueServer.AliyunAccessKeySecret, config.getProperty("mns.accesskeysecret", ""));
        //
        setValue(QueueServer.RMQAccountEndpoint, config.getProperty("rocketmq.endpoint", ""));
        setValue(QueueServer.RMQInstanceId, config.getProperty("rocketmq.instanceId", ""));
        setValue(QueueServer.RMQEndpoint, config.getProperty("rocketmq.queue.endpoint", ""));
        setValue(QueueServer.RMQAccessKeyId, config.getProperty("rocketmq.queue.accesskeyid", ""));
        setValue(QueueServer.RMQAccessKeySecret, config.getProperty("rocketmq.queue.accesskeysecret", ""));
    }
}
