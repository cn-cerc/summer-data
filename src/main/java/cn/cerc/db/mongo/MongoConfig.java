package cn.cerc.db.mongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.zk.ZkNode;

@Component
public class MongoConfig {
    private static final Logger log = LoggerFactory.getLogger(MongoConfig.class);

    private static final ServerConfig config = ServerConfig.getInstance();

    private static final String prefix = String.format("/%s/%s/mongodb/", ServerConfig.getAppProduct(),
            ServerConfig.getAppVersion());

    private static volatile MongoClient client;

    /**
     * 不同数据库的客户端
     */
    private static MongoClient getClient() {
        if (client != null)
            return client;

        var username = ZkNode.get()
                .getNodeValue(prefix + "username", () -> config.getProperty("mgdb.username", "mongodb_user"));
        var password = ZkNode.get()
                .getNodeValue(prefix + "password", () -> config.getProperty("mgdb.password", "mongodb_password"));
        var maxpoolsize = ZkNode.get()
                .getNodeValue(prefix + "maxpoolsize", () -> config.getProperty("mgdb.maxpoolsize", "100"));// 单客户端默认最大100个连接
        var hosts = ZkNode.get()
                .getNodeValue(prefix + "hosts", () -> config.getProperty("mgdb.ipandport",
                        "mongodb.local.top:27018,mongodb.local.top:27019,mongodb.local.top:27020"));

        synchronized (MongoConfig.class) {
            if (client == null) {
                StringBuilder builder = new StringBuilder();
                builder.append("mongodb://")
                        .append(username)
                        .append(":")
                        .append(password)
                        .append("@")
                        .append(hosts)
                        .append("/");

                // 是否启用集群模式
                builder.append("?").append("maxPoolSize=").append(maxpoolsize);
                builder.append("&").append("connectTimeoutMS=").append("3000");
                builder.append("&").append("serverSelectionTimeoutMS=").append("3000");
                log.info("Connect to the MongoDB sharded cluster {}", builder);

                ConnectionString connection = new ConnectionString(builder.toString());
                client = MongoClients.create(connection);
            }
        }
        return client;
    }

    /**
     * 获取默认的数据库名称 databaseName
     */
    public static MongoDatabase getDatabase() {
        return MongoConfig.getDatabase("");
    }

    /**
     * 获取指定的数据库名称
     *
     * @param suffix 业务类型后缀，例如 _gps<br>
     *               组合以后变成 4plc_gps
     */
    public static MongoDatabase getDatabase(String suffix) {
        String databaseName = ZkNode.get()
                .getNodeValue(prefix + "database", () -> config.getProperty("mgdb.dbname", "mongodb_database"));
        if (Utils.isEmpty(databaseName))
            throw new RuntimeException("MongoDB database name is empty.");
        if (!Utils.isEmpty(suffix))
            databaseName = String.join("_", databaseName, suffix);
        return MongoConfig.getClient().getDatabase(databaseName);
    }

    public static void close() {
        MongoConfig.client.close();
        log.warn("mongodb client 已关闭");
    }

}