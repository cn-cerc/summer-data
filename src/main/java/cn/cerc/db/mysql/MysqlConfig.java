package cn.cerc.db.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.zk.ZkNode;

public class MysqlConfig {
    private static final Logger log = LoggerFactory.getLogger(MysqlConfig.class);
    // mysql驱动
    public static final String JdbcDriver;
    private static MysqlConfig instanceMaster;
    private static MysqlConfig instanceSalve;
    private static final ServerConfig config;
    private final ZkNode node = ZkNode.get();
    private String slaveFlag = "";

    static {
        config = ServerConfig.getInstance();
        JdbcDriver = config.getProperty("spring.datasource.driver-class-name", "com.mysql.cj.jdbc.Driver");
    }

    public synchronized static MysqlConfig getMaster() {
        if (instanceMaster == null)
            instanceMaster = new MysqlConfig(true);
        return instanceMaster;
    }

    public synchronized static MysqlConfig getSlave() {
        if (instanceSalve == null)
            instanceSalve = new MysqlConfig(false);
        return instanceSalve;
    }

    private MysqlConfig(boolean isMaster) {
        if (isMaster) {
            instanceMaster = this;
            slaveFlag = "";
        } else {
            instanceSalve = this;
            slaveFlag = ".slave";
        }
    }

    public String site() {
        return node.getString(getNodePath("site"), () -> {
            return config.getProperty("rds.site", "mysql.local.top:3306");
        });
    }

    public String database() {
        return node.getString(getNodePath("database"), () -> {
            return config.getProperty("rds.database", "appdb");
        });
    }

    public String username() {
        return node.getString(getNodePath("username"), () -> {
            return config.getProperty("rds.username", "appdb_user");
        });
    }

    public String password() {
        return node.getString(getNodePath("password"), () -> {
            return config.getProperty("rds.password", "appdb_password");
        });
    }

    public String serverTimezone() {
        return node.getString(getNodePath("serverTimezone"), () -> "Asia/Shanghai");
    }

    /**
     * 连接池最大连接数，默认为0（不启用），建议设置为最大并发请求数量
     *
     * @return maxPoolSize
     */
    public int maxPoolSize() {
        return node.getInt(getNodePath("MaxPoolSize"), 0);
    }

    /**
     * 连接池最小连接数，默认为9，即CPU核心数*2+1
     *
     * @return minPoolSize
     */
    public int minPoolSize() {
        return node.getInt(getNodePath("MinPoolSize"), 10);
    }

    /**
     * 连接池在建立时即初始化的连接数量
     *
     * @return initialPoolSize
     */
    public int initialPoolSize() {
        return node.getInt(getNodePath("InitialPoolSize"), 3);
    }

    /**
     * 设置创建连接超时时间，单位为毫秒，默认为0.5秒，此值建议设置为不良体验值（当前为超出1秒即警告）的一半
     *
     * @return checkoutTimeout
     */
    public int checkoutTimeout() {
        return node.getInt(getNodePath("CheckoutTimeout"), 500);
    }

    /**
     * 检查连接池中所有连接的空闲，单位为秒。注意MySQL空闲超过8小时连接自动关闭） 默认为空闲2小时即自动断开，建议其值为
     * tomcat.session的生存时长(一般设置为120分钟) 加10分钟，即120 * 60 = 7200
     *
     * 单位毫秒， HikariCP 默认是 600000ms
     *
     * @return maxIdleTime
     */
    public int maxIdleTime() {
        return node.getInt(getNodePath("MaxIdleTime"), 600000);
    }

    /**
     * 检查连接池中所有空闲连接的间隔时间，单位为秒。默认为9秒，其值应比 mysql 的connect_timeout默认为10秒少1秒，即9秒
     *
     * @return idleConnectionTestPeriod
     */
    public int idleConnectionTestPeriod() {
        return node.getInt(getNodePath("IdleConnectionTestPeriod"), 9);
    }

    private String getNodePath(String key) {
        return String.format("mysql/%s%s", Utils.isEmpty(slaveFlag) ? "main/" : "slave/", key);
    }

    public boolean isConfigNull() {
        return Utils.isEmpty(site()) || Utils.isEmpty(database()) || Utils.isEmpty(serverTimezone());
    }

    public String getConnectUrl() {
        if (isConfigNull())
            throw new RuntimeException("mysql connection config is null");

        return String.format(
                "jdbc:mysql://%s/%s?useSSL=false&autoReconnect=true&autoCommit=false&useUnicode=true&characterEncoding=utf8&serverTimezone=%s&zeroDateTimeBehavior=CONVERT_TO_NULL",
                site(), database(), serverTimezone());
    }

    /**
     * 创建连接池
     */
    public final HikariDataSource createDataSource() {
        log.info("create pool to {}", site());
        String datahost = site();
        String database = database();
        String timezone = serverTimezone();
        if (Utils.isEmpty(datahost) || Utils.isEmpty(database) || Utils.isEmpty(timezone))
            throw new RuntimeException("mysql connection config is null");

        var jdbcUrl = String.format(
                "jdbc:mysql://%s/%s?useSSL=false&autoReconnect=true&autoCommit=false&useUnicode=true&characterEncoding=utf8&serverTimezone=%s&zeroDateTimeBehavior=CONVERT_TO_NULL",
                datahost, database, timezone);

        HikariConfig config = new HikariConfig();
        config.setDriverClassName(MysqlConfig.JdbcDriver);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username());
        config.setPassword(password());
        config.setMaximumPoolSize(maxPoolSize()); // 连接池的最大连接数
        config.setMinimumIdle(minPoolSize()); // 连接池的最小空闲连接数
        config.setIdleTimeout(maxIdleTime());// 连接在池中闲置的最长时间
//        config.addDataSourceProperty("cachePrepStmts", "true");// 启用缓存PreparedStatement对象
//        config.addDataSourceProperty("prepStmtCacheSize", "250"); // 连接池中可以缓存的PreparedStatement对象的最大数量
//        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048"); // 允许缓存的SQL语句的最大长度
        return new HikariDataSource(config);
    }

    /**
     * 不使用线程池直接创建连接
     * 
     * @return 数据库连接
     */
    public Connection createConnection() {
        return this.createConnection(site(), database(), username(), password());
    }

    /**
     * 根据传入的参数直接创建连接
     * 
     * @param host     数据库地址
     * @param database 数据库名称
     * @param username 用户名称
     * @param password 用户密码
     * @return 数据库连接
     */
    public Connection createConnection(String host, String database, String username, String password) {
        var timezone = serverTimezone();
        if (Utils.isEmpty(host) || Utils.isEmpty(database) || Utils.isEmpty(timezone))
            throw new RuntimeException("mysql connection config is null");
        var jdbcUrl = String.format(
                "jdbc:mysql://%s/%s?useSSL=false&autoReconnect=true&autoCommit=false&useUnicode=true&characterEncoding=utf8&serverTimezone=%s&zeroDateTimeBehavior=CONVERT_TO_NULL",
                host, database, timezone);
        try {
            Class.forName(MysqlConfig.JdbcDriver);
            log.debug("连接到 mysql, host={}, database={}, username={}", host, database, username);
            return DriverManager.getConnection(jdbcUrl, username, password);
        } catch (SQLException | ClassNotFoundException e) {
            log.error("connection {}, database {}", jdbcUrl, database, e);
            throw new RuntimeException(e);
        }
    }

}
