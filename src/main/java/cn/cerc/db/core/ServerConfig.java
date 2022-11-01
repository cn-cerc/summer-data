package cn.cerc.db.core;

import java.util.Properties;

import cn.cerc.db.SummerDB;
import cn.cerc.db.queue.QueueServiceEnum;

public enum ServerConfig implements IConfig {

    INSTANCE;

    private static final ClassConfig config = new ClassConfig(ServerConfig.class, SummerDB.ID);

    public static ServerConfig getInstance() {
        return INSTANCE;
    }

    // 是否为debug状态
    private int debug = -1;

    public static boolean enableTaskService() {
        return config.getBoolean("task.service", false);
    }

    /**
     * 
     * @return 返回消息服务的类型， Redis/RocketMQ
     */
    public static QueueServiceEnum getQueueService() {
        var result = config.getString("queue.service", "RocketMQ").toLowerCase();
        if ("redis".equals(result))
            return QueueServiceEnum.Redis;
        else
            return QueueServiceEnum.RocketMQ;
    }

    /**
     * 
     * @return 主机代码，全网络唯一
     */
    public static String getAppName() {
        return config.getString("appName", "localhost");
    }

    /**
     * 
     * @return 产品代码
     */
    public static String getAppProduct() {
        return config.getString("application.product", "summer");
    }

    /**
     * 
     * @return 产品版本：develop/beta/main
     */
    public static String getAppVersion() {
        return config.getString("version", "develop");
    }

    /**
     * 
     * @return 产业：csp/obm/odm/oem/fpl
     */
    public static String getAppIndustry() {
        return config.getString("application.original", "std");
    }

    public static boolean enableDocService() {
        return config.getBoolean("docs.service", false);
    }

    // 正式环境
    public static boolean isServerMaster() {
        String value = config.getString("version", "develop");
        if ("release".equals(value))
            return true;
        return "master".equals(value);
    }

    // 测试环境
    public static boolean isServerBeta() {
        return "beta".equals(config.getString("version", "develop"));
    }

    // alpha环境
    public static boolean isServerAlpha() {
        return "alpha".equals(config.getString("version", "develop"));
    }

    // 开发环境
    public static boolean isServerDevelop() {
        if (isServerMaster())
            return false;
        if (isServerBeta())
            return false;
        if (isServerAlpha())
            return false;
        return true;
    }

    /**
     * 读取配置，请改为使用 ClassConfig
     */
    @Override
    public String getProperty(String key, String def) {
        return config.getString(key, def);
    }

    public Properties loadAll() {
        return config.getProperties();
    }

    /**
     * @return 返回当前是否为debug状态
     */
    public boolean isDebug() {
        if (debug == -1) {
            debug = config.getBoolean("debug", false) ? 1 : 0;
        }
        return debug == 1;
    }

}
