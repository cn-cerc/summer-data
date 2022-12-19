package cn.cerc.db.mongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.zk.ZkNode;

@Component
public class MongoConfig {
    private static final Logger log = LoggerFactory.getLogger(MongoConfig.class);

    public static final String mongodb_database = "mgdb.dbname";
    public static final String mongodb_username = "mgdb.username";
    public static final String mongodb_password = "mgdb.password";
    public static final String mgdb_site = "mgdb.ipandport";
    public static final String mgdb_enablerep = "mgdb.enablerep";
    public static final String mgdb_replicaset = "mgdb.replicaset";
    public static final String mgdb_maxpoolsize = "mgdb.maxpoolsize";
//    public static final String SessionId = "mongoSession";

    private static final IConfig config = ServerConfig.getInstance();
    private static volatile MongoClient client;

    public MongoClient getClient() {
        if (client == null) {
            synchronized (MongoConfig.class) {
                final String prefix = String.format("/%s/%s/mongodb", ServerConfig.getAppProduct(),ServerConfig.getAppVersion());
                var username = ZkNode.get()
                        .getNodeValue(prefix + "username", () -> config.getProperty(MongoConfig.mongodb_username));
                var password = ZkNode.get()
                        .getNodeValue(prefix + "password", () -> config.getProperty(MongoConfig.mongodb_password));
                var ipandport = ZkNode.get()
                        .getNodeValue(prefix + "ipandport", () -> config.getProperty(MongoConfig.mgdb_site));
                var database = ZkNode.get()
                        .getNodeValue(prefix + "database", () -> config.getProperty(MongoConfig.mongodb_database));
                var enablerep = ZkNode.get()
                        .getNodeValue(prefix + "enablerep", () -> config.getProperty(MongoConfig.mgdb_enablerep));
                var replicaset = ZkNode.get()
                        .getNodeValue(prefix + "replicaset", () -> config.getProperty(MongoConfig.mgdb_replicaset));
                var maxpoolsize = ZkNode.get()
                        .getNodeValue(prefix + "maxpoolsize", () -> config.getProperty(MongoConfig.mgdb_maxpoolsize));
                var connectTimeoutMS = ZkNode.get().getNodeValue(prefix + "connectTimeoutMS", () -> "3000");
                var serverSelectionTimeoutMS = ZkNode.get()
                        .getNodeValue(prefix + "serverSelectionTimeoutMS", () -> "3000");

                StringBuilder builder = new StringBuilder();
                builder.append("mongodb://");
                // userName
                builder.append(username);
                // password
                builder.append(":").append(password);
                // ip
                builder.append("@").append(ipandport);
                // database
                builder.append("/").append(database);

                if ("true".equals(enablerep)) {
                    // replacaset
                    builder.append("?").append("replicaSet=").append(replicaset);
                    // poolsize
                    builder.append("&").append("maxPoolSize=").append(maxpoolsize);
                    builder.append("&").append("connectTimeoutMS=").append(connectTimeoutMS);
                    builder.append("&").append("serverSelectionTimeoutMS=").append(serverSelectionTimeoutMS);
                    log.info("Connect to the MongoDB sharded cluster {}", builder);
                }
                ConnectionString connectionString = new ConnectionString(builder.toString());
                client = MongoClients.create(connectionString);
            }
        }
        return client;
    }

    public String databaseName() {
         String prefix = String.format("/%s/%s/mongodb", ServerConfig.getAppProduct(),ServerConfig.getAppVersion());
        String databaseName = ZkNode.get()
                .getNodeValue(prefix + "dbname", () -> config.getProperty(MongoConfig.mongodb_database));
        if (Utils.isEmpty(databaseName))
            throw new RuntimeException("MongoDB database name is empty.");
        return databaseName;
    }

}