package cn.cerc.db.queue;

import cn.cerc.db.SummerDB;
import cn.cerc.db.core.ClassResource;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.zk.ZkNode;

public class AliyunUserConfig {
    private static final ClassResource res = new ClassResource(MnsServer.class, SummerDB.ID);
    private static final String AccountEndpoint = "aliyunMNS/accountendpoint";
    private static final String AccessKeyId = "aliyunMNS/accesskeyid";
    private static final String AccessKeySecret = "aliyunMNS/accesskeysecret";
    private static ServerConfig config = ServerConfig.getInstance();

    public static String accessKeyId() {
        var result = ZkNode.get().getString(AccessKeyId, config.getProperty("mns.accesskeyid"));
        if (Utils.isEmpty(result))
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), AccessKeyId));
        return result;
    }

    public static String accessKeySecret() {
        var result = ZkNode.get().getString(AccessKeySecret, config.getProperty("mns.accesskeysecret"));
        if (Utils.isEmpty(result))
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), AccessKeySecret));
        return result;
    }

    public static String accountEndpoint() {
        var result = ZkNode.get().getString(AccountEndpoint, config.getProperty("mns.accountendpoint"));
        if (Utils.isEmpty(result))
            throw new RuntimeException(String.format(res.getString(1, "%s 配置为空"), AccountEndpoint));
        return result;
    }
}
