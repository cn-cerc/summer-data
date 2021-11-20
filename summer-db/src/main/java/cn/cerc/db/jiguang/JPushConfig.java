package cn.cerc.db.jiguang;

import cn.cerc.core.IConfig;
import cn.cerc.db.core.ServerConfig;
import cn.jpush.api.JPushClient;

public class JPushConfig {

    // 配置文件
    public static final String masterSecret = "jiguang.masterSecret";
    public static final String appKey = "jiguang.appKey";
    private static volatile JPushClient client;

    public static JPushClient getClient() {
        if (client == null) {
            synchronized (JPushConfig.class) {
                if (client == null) {
                    IConfig config = ServerConfig.getInstance();
                    String masterSecret = config.getProperty(JPushConfig.masterSecret);
                    if (masterSecret == null) {
                        throw new RuntimeException("jiguang.masterSecret is null");
                    }
                    String appKey = config.getProperty(JPushConfig.appKey);
                    if (appKey == null) {
                        throw new RuntimeException("jiguang.appKey is null");
                    }
                    client = new JPushClient(masterSecret, appKey);
                }
            }
        }
        return client;
    }
}
