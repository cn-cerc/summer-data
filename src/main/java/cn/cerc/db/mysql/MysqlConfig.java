package cn.cerc.db.mysql;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.zk.ZkNode;

public class MysqlConfig {
    private static final Logger log = LoggerFactory.getLogger(MysqlConfig.class);
    // mysql驱动
    public static final String JdbcDriver;
    private static MysqlConfig instanceMaster;
    private static MysqlConfig instanceSalve;
    private static ServerConfig config;
    private ZkNode node = ZkNode.get();
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
        return node.getString(getNodePath("site"), () -> "mysql.local.top:3306");
    }

    public String database() {
        return node.getString(getNodePath("database"), () -> "appdb");
    }

    public String username() {
        return node.getString(getNodePath("username"), () -> "appdb_user");
    }

    public String password() {
        return node.getString(getNodePath("password"), () -> "appdb_password");
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
        return node.getInt(getNodePath("MinPoolSize"), 9);
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
     * tomcat.session的生存时长(一般设置为120分钟) 加10分钟，即130 * 60 = 7800
     * 
     * @return maxIdleTime
     */
    public int maxIdleTime() {
        return node.getInt(getNodePath("MaxIdleTime"), 7800);
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
        return String.format("mysql/%s%s", Utils.isEmpty(slaveFlag) ? "" : "slave/", key);
    }

    public boolean isConfigNull() {
        return Utils.isEmpty(site()) || Utils.isEmpty(database()) || Utils.isEmpty(serverTimezone());
    }

    public String getConnectUrl() {
        if (isConfigNull())
            throw new RuntimeException("mysql connection config is null");

        return String.format(
                "jdbc:mysql://%s/%s?useSSL=false&autoReconnect=true&autoCommit=false&useUnicode=true&characterEncoding=utf8&serverTimezone=%s",
                site(), database(), serverTimezone());
    }

    public final ComboPooledDataSource createDataSource() {
        log.info("create pool to: " + site());
        // 使用线程池创建
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass(MysqlConfig.JdbcDriver);
        } catch (PropertyVetoException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        var host = site();
        var database = database();
        var timezone = serverTimezone();
        if (Utils.isEmpty(host) || Utils.isEmpty(database) || Utils.isEmpty(timezone))
            throw new RuntimeException("mysql connection config is null");

        var jdbcUrl = String.format(
                "jdbc:mysql://%s/%s?useSSL=false&autoReconnect=true&autoCommit=false&useUnicode=true&characterEncoding=utf8&serverTimezone=%s",
                host, database, timezone);

        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUser(username());
        dataSource.setPassword(password());
        // 连接池大小设置
        dataSource.setMaxPoolSize(maxPoolSize());
        dataSource.setMinPoolSize(minPoolSize());
        dataSource.setInitialPoolSize(initialPoolSize());
        // 连接池断开控制
        dataSource.setCheckoutTimeout(checkoutTimeout()); // 单位毫秒
        dataSource.setMaxIdleTime(maxIdleTime()); // 空闲自动断开时间
        // 每隔多少时间（时间请小于 数据库的 timeout）,测试一下链接，防止失效，会损失小部分性能
        dataSource.setIdleConnectionTestPeriod(idleConnectionTestPeriod()); // 单位秒
        dataSource.setTestConnectionOnCheckin(true);
        dataSource.setTestConnectionOnCheckout(false);
        return dataSource;
    }

    public Connection createConnection() {
        return this.createConnection(site(), database(), username(), password());
    }

    public Connection createConnection(String host, String database, String username, String password) {
        var timezone = serverTimezone();
        if (Utils.isEmpty(host) || Utils.isEmpty(database) || Utils.isEmpty(timezone))
            throw new RuntimeException("mysql connection config is null");
        var jdbcUrl = String.format(
                "jdbc:mysql://%s/%s?useSSL=false&autoReconnect=true&autoCommit=false&useUnicode=true&characterEncoding=utf8&serverTimezone=%s",
                host, database, timezone);
        try {
            Class.forName(MysqlConfig.JdbcDriver);
            return DriverManager.getConnection(jdbcUrl, username, password);
        } catch (SQLException | ClassNotFoundException e) {
            log.error("mysql connection {} database {}", jdbcUrl, database);
            throw new RuntimeException(e);
        }
    }

}
