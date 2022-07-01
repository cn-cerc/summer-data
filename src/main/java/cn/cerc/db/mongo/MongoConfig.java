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

    public static MongoClient getClient() {
        if (client == null) {
            synchronized (MongoConfig.class) {
                StringBuilder builder = new StringBuilder();
                builder.append("mongodb://");
                // userName
                builder.append(config.getProperty(MongoConfig.mongodb_username));
                // password
                builder.append(":").append(config.getProperty(MongoConfig.mongodb_password));
                // ip
                builder.append("@").append(config.getProperty(MongoConfig.mgdb_site));
                // database
                builder.append("/").append(config.getProperty(MongoConfig.mongodb_database));

                if ("true".equals(config.getProperty(MongoConfig.mgdb_enablerep))) {
                    // replacaset
                    builder.append("?").append("replicaSet=").append(config.getProperty(MongoConfig.mgdb_replicaset));
                    // poolsize
                    builder.append("&").append("maxPoolSize=").append(config.getProperty(MongoConfig.mgdb_maxpoolsize));
                    builder.append("&").append("connectTimeoutMS=").append("3000");
                    builder.append("&").append("serverSelectionTimeoutMS=").append("3000");
                    log.info("Connect to the MongoDB sharded cluster {}", builder);
                }
                ConnectionString connectionString = new ConnectionString(builder.toString());
                client = MongoClients.create(connectionString);
            }
        }
        return client;
    }

    public static String databaseName() {
        String databaseName = config.getProperty(MongoConfig.mongodb_database);
        if (Utils.isEmpty(databaseName))
            throw new RuntimeException("MongoDB database name is empty.");
        return databaseName;
    }

}