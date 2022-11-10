package cn.cerc.db.zk;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.queue.MnsServer;
import cn.cerc.db.queue.QueueServer;

public class ZkConfig implements IConfig {
    private static final Logger log = LoggerFactory.getLogger(ZkConfig.class);
    private static final IConfig config = ServerConfig.getInstance();
    private ZkServer server;
    private String path;

    public ZkConfig(String path) {
        super();
        if (Utils.isEmpty(path))
            throw new RuntimeException("path 不允许为空");
        if (path.endsWith("/"))
            throw new RuntimeException("path 不得以 / 结尾");

        this.path = String.format("/%s/%s/%s%s", ServerConfig.getAppProduct(), ServerConfig.getAppVersion(),
                ServerConfig.getAppIndustry(), path);

        server = ZkNode.get().server();
        if ("/aliyunMNS".equals(path) && !this.exists())
            this.fixAliyunMNS();
        if ("/rocketMQ".equals(path) && !this.exists())
            this.fixRocketMQ();
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
        String result = ZkNode.get().getNodeValue(path(key), def);
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

    /**
     * 用于结转旧的配置文件
     * 
     * @param config
     */
    private void fixAliyunMNS() {
        log.warn("fixAliyunMNS: 自动结转旧的配置数据");
        setValue(MnsServer.AccountEndpoint, config.getProperty("mns.accountendpoint"));
        setValue(MnsServer.AccessKeyId, config.getProperty("mns.accesskeyid"));
        setValue(MnsServer.AccessKeySecret, config.getProperty("mns.accesskeysecret"));
        setValue(MnsServer.SecurityToken, config.getProperty("mns.securitytoken"));
    }

    /**
     * 用于结转旧的配置文件
     * 
     * @param config
     */
    private void fixRocketMQ() {
        log.warn("fixRocketMQ: 自动结转旧的配置数据");
        setValue(QueueServer.AliyunAccessKeyId, config.getProperty("mns.accesskeyid"));
        setValue(QueueServer.AliyunAccessKeySecret, config.getProperty("mns.accesskeysecret"));
        //
        setValue(QueueServer.RMQAccountEndpoint, config.getProperty("rocketmq.endpoint"));
        setValue(QueueServer.RMQInstanceId, config.getProperty("rocketmq.instanceId"));
        setValue(QueueServer.RMQEndpoint, config.getProperty("rocketmq.queue.endpoint"));
        setValue(QueueServer.RMQAccessKeyId, config.getProperty("rocketmq.queue.accesskeyid"));
        setValue(QueueServer.RMQAccessKeySecret, config.getProperty("rocketmq.queue.accesskeysecret"));
    }

    public ZooKeeper client() {
        return server.client();
    }

    @Deprecated
    public ZkServer server() {
        return ZkNode.get().server();
    }
}
