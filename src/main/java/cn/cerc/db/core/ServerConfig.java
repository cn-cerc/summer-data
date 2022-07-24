package cn.cerc.db.core;

import cn.cerc.db.SummerDB;

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

    public static String getAppName() {
        return config.getString("appName", "localhost");
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
    public static boolean isServerAplha() {
        return "alpha".equals(config.getString("version", "develop"));
    }

    // 开发环境
    public static boolean isServerDevelop() {
        if (isServerMaster())
            return false;
        if (isServerBeta())
            return false;
        if (isServerAplha())
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
