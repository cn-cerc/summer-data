package cn.cerc.db.zk;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.mysql.MysqlConfig;
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
        if ("/mysql".equals(path) && !this.exists())
            this.fixMysqlMaster();
        if ("/mysql/slave".equals(path) && !this.exists())
            this.fixMysqlSlave();

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
        if (result != null) {
            return result;
        } else {
            this.setValue(key, def);
            return def;
        }
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
        server.setValue(createKey(path, key), value == null ? "" : value);
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
        setValue(QueueServer.AliyunAccessKeyId, config.getProperty("mns.accesskeyid"));
        setValue(QueueServer.AliyunAccessKeySecret, config.getProperty("mns.accesskeysecret"));
        //
        setValue(QueueServer.RMQAccountEndpoint, config.getProperty("rocketmq.endpoint"));
        setValue(QueueServer.RMQInstanceId, config.getProperty("rocketmq.instanceId"));
        setValue(QueueServer.RMQEndpoint, config.getProperty("rocketmq.queue.endpoint"));
        setValue(QueueServer.RMQAccessKeyId, config.getProperty("rocketmq.queue.accesskeyid"));
        setValue(QueueServer.RMQAccessKeySecret, config.getProperty("rocketmq.queue.accesskeysecret"));
    }

    private void fixMysqlMaster() {
        log.warn("fixMysql: 自动结转旧的配置数据-主库");
        setValue(MysqlConfig.rds_site, config.getProperty("rds.site", "127.0.0.1:3306"));
        setValue(MysqlConfig.rds_database, config.getProperty("rds.database", "appdb"));
        setValue(MysqlConfig.rds_username, config.getProperty("rds.username", "appdb_user"));
        setValue(MysqlConfig.rds_password, config.getProperty("rds.password", "appdb_password"));
        //
        setValue(MysqlConfig.rds_ServerTimezone, config.getProperty("rds.serverTimezone", "Asia/Shanghai"));
        setValue(MysqlConfig.rds_MaxPoolSize, config.getProperty("rds.MaxPoolSize", "0"));
        setValue(MysqlConfig.rds_MinPoolSize, config.getProperty("rds.MinPoolSize", "9"));
        setValue(MysqlConfig.rds_InitialPoolSize, config.getProperty("rds.InitialPoolSize", "3"));
        setValue(MysqlConfig.rds_CheckoutTimeout, config.getProperty("rds.CheckoutTimeout", "500"));
        setValue(MysqlConfig.rds_MaxIdleTime, config.getProperty("rds.MaxIdleTime", "7800"));
        setValue(MysqlConfig.rds_IdleConnectionTestPeriod, config.getProperty("rds.IdleConnectionTestPeriod", "9"));
    }

    private void fixMysqlSlave() {
        log.warn("fixMysql: 自动结转旧的配置数据-从库");
        ZkConfig zkc = new ZkConfig("/mysql");
        setValue(MysqlConfig.rds_site, config.getProperty("rds.site.slave", zkc.getProperty(MysqlConfig.rds_site)));
        setValue(MysqlConfig.rds_database,
                config.getProperty("rds.database.slave", zkc.getProperty(MysqlConfig.rds_database)));
        setValue(MysqlConfig.rds_username,
                config.getProperty("rds.username.slave", zkc.getProperty(MysqlConfig.rds_username)));
        setValue(MysqlConfig.rds_password,
                config.getProperty("rds.password.slave", zkc.getProperty(MysqlConfig.rds_password)));
        //
        setValue(MysqlConfig.rds_ServerTimezone,
                config.getProperty("rds.serverTimezone.slave", zkc.getProperty(MysqlConfig.rds_ServerTimezone)));
        setValue(MysqlConfig.rds_MaxPoolSize,
                config.getProperty("rds.MaxPoolSize.slave", zkc.getProperty(MysqlConfig.rds_MaxPoolSize)));
        setValue(MysqlConfig.rds_MinPoolSize,
                config.getProperty("rds.MinPoolSize.slave", zkc.getProperty(MysqlConfig.rds_MinPoolSize)));
        setValue(MysqlConfig.rds_InitialPoolSize,
                config.getProperty("rds.InitialPoolSize.slave", zkc.getProperty(MysqlConfig.rds_InitialPoolSize)));
        setValue(MysqlConfig.rds_CheckoutTimeout,
                config.getProperty("rds.CheckoutTimeout.slave", zkc.getProperty(MysqlConfig.rds_CheckoutTimeout)));
        setValue(MysqlConfig.rds_MaxIdleTime,
                config.getProperty("rds.MaxIdleTime.slave", zkc.getProperty(MysqlConfig.rds_MaxIdleTime)));
        setValue(MysqlConfig.rds_IdleConnectionTestPeriod, config.getProperty("rds.IdleConnectionTestPeriod.slave",
                zkc.getProperty(MysqlConfig.rds_IdleConnectionTestPeriod)));
    }
}
