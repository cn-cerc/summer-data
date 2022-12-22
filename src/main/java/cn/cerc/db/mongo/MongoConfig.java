package cn.cerc.db.mongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.zk.ZkNode;

@Component
public class MongoConfig {
    private static final Logger log = LoggerFactory.getLogger(MongoConfig.class);

    private static final String prefix = String.format("/%s/%s/mongodb/", ServerConfig.getAppProduct(),
            ServerConfig.getAppVersion());

    private static volatile MongoClient client;

    public static MongoClient getClient() {
        if (client != null)
            return client;

        var username = ZkNode.get().getNodeValue(prefix + "username", () -> "mongodb_user");
        var password = ZkNode.get().getNodeValue(prefix + "password", () -> "mongodb_password");
        var database = MongoConfig.database();
        var enablerep = ZkNode.get().getNodeValue(prefix + "enablerep", () -> "true");
        var maxpoolsize = ZkNode.get().getNodeValue(prefix + "maxpoolsize", () -> "100");// 单客户端默认最大100个连接
        var hosts = ZkNode.get()
                .getNodeValue(prefix + "hosts",
                        () -> "mongodb.local.top:27018,mongodb.local.top:27019,mongodb.local.top:27020");

        synchronized (MongoConfig.class) {
            StringBuilder builder = new StringBuilder();
            builder.append("mongodb://")
                    .append(username)
                    .append(":")
                    .append(password)
                    .append("@")
                    .append(hosts)
                    .append("/")
                    .append(database);

            if ("true".equals(enablerep)) {
                builder.append("?").append("maxPoolSize=").append(maxpoolsize);
                builder.append("&").append("connectTimeoutMS=").append("3000");
                builder.append("&").append("serverSelectionTimeoutMS=").append("3000");
                log.info("Connect to the MongoDB sharded cluster {}", builder);
            }

            ConnectionString connection = new ConnectionString(builder.toString());
            client = MongoClients.create(connection);
        }
        return client;
    }

    public static String database() {
        String database = ZkNode.get().getNodeValue(prefix + "database", () -> "mongodb_database");
        if (Utils.isEmpty(database))
            throw new RuntimeException("MongoDB database name is empty.");
        return database;
    }

}