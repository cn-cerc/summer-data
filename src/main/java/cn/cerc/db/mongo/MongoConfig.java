package cn.cerc.db.mongo;

import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import com.mongodb.ConnectionString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
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

    private static String databaseName;
    private static final IConfig config = ServerConfig.getInstance();

    public static ConnectionString getUri() {
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
        return new ConnectionString(builder.toString());
    }

    public static String databaseName() {
        String databaseName = config.getProperty(MongoConfig.mongodb_database);
        if (Utils.isEmpty(databaseName))
            throw new RuntimeException("MongoDB database name is empty.");
        return databaseName;
    }

}