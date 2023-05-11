package cn.cerc.db.mongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.mongodb.ConnectionString;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.zk.ZkNode;

@Component
public class MongoConfig {
    private static final Logger log = LoggerFactory.getLogger(MongoConfig.class);

    private static final ServerConfig config = ServerConfig.getInstance();

    private static final String prefix = String.format("/%s/%s/mongodb/", ServerConfig.getAppProduct(),
            ServerConfig.getAppVersion());

    private volatile static ConnectionString connection;

    public static ConnectionString connection() {
        if (connection != null)
            return connection;

        synchronized (MongoConfig.class) {
            if (connection == null) {
                var username = ZkNode.get()
                        .getNodeValue(prefix + "username", () -> config.getProperty("mgdb.username", "mongodb_user"));
                var password = ZkNode.get()
                        .getNodeValue(prefix + "password",
                                () -> config.getProperty("mgdb.password", "mongodb_password"));
                var database = MongoConfig.database();
                var maxpoolsize = ZkNode.get()
                        .getNodeValue(prefix + "maxpoolsize", () -> config.getProperty("mgdb.maxpoolsize", "100"));// 单客户端默认最大100个连接
                var hosts = ZkNode.get()
                        .getNodeValue(prefix + "hosts", () -> config.getProperty("mgdb.ipandport",
                                "mongodb.local.top:27018,mongodb.local.top:27019,mongodb.local.top:27020"));

                StringBuilder builder = new StringBuilder();
                builder.append("mongodb://")
                        .append(username)
                        .append(":")
                        .append(password)
                        .append("@")
                        .append(hosts)
                        .append("/")
                        .append(database);

                // 是否启用集群模式
                builder.append("?").append("maxPoolSize=").append(maxpoolsize);
                builder.append("&").append("connectTimeoutMS=").append("3000");
                builder.append("&").append("serverSelectionTimeoutMS=").append("3000");
                log.info("Connect to the MongoDB sharded cluster {}", builder);
                connection = new ConnectionString(builder.toString());
            }
        }
        return connection;
    }

    public static String database() {
        String database = ZkNode.get()
                .getNodeValue(prefix + "database", () -> config.getProperty("mgdb.dbname", "mongodb_database"));
        if (Utils.isEmpty(database))
            throw new RuntimeException("MongoDB database name is empty.");
        return database;
    }

}