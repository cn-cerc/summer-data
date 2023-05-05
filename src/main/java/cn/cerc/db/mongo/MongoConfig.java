package cn.cerc.db.mongo;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
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

    public static MongoClient getClient() {
        if (client != null)
            return client;

        var username = ZkNode.get()
                .getNodeValue(prefix + "username", () -> config.getProperty("mgdb.username", "mongodb_user"));
        var password = ZkNode.get()
                .getNodeValue(prefix + "password", () -> config.getProperty("mgdb.password", "mongodb_password"));
        var database = MongoConfig.database();
        var enablerep = ZkNode.get()
                .getNodeValue(prefix + "enablerep", () -> config.getProperty("mgdb.enablerep", "false"));
        var maxpoolsize = ZkNode.get()
                .getNodeValue(prefix + "maxpoolsize", () -> config.getProperty("mgdb.maxpoolsize", "100"));// 单客户端默认最大100个连接
        var hosts = ZkNode.get()
                .getNodeValue(prefix + "hosts", () -> config.getProperty("mgdb.ipandport",
                        "mongodb.local.top:27018,mongodb.local.top:27019,mongodb.local.top:27020"));

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

            // 是否启用集群模式
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
        String database = ZkNode.get()
                .getNodeValue(prefix + "database", () -> config.getProperty("mgdb.dbname", "mongodb_database"));
        if (Utils.isEmpty(database))
            throw new RuntimeException("MongoDB database name is empty.");
        return database;
    }

    public static MongoCollection<Document> getCollection(String collectionName) {
        MongoClient mongoClient = MongoConfig.getClient();
        MongoDatabase database = mongoClient.getDatabase(MongoConfig.database());
        return database.getCollection(collectionName);
    }

}