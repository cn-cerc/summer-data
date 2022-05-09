package cn.cerc.db.mysql;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.resourcepool.TimeoutException;

import cn.cerc.db.core.ServerClient;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISqlServer;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public abstract class MysqlServer implements ISqlServer, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MysqlServer.class);
    private Connection connection;
    private MysqlClient client;
    // 取得所有的表名
    private List<String> tables;
    // 标记栏位，为兼容历史delphi写法
    private int tag;

    @Override
    public abstract String getHost();

    public abstract String getDatabase();

    @Override
    public final MysqlClient getClient() {
        if (client == null)
            client = new MysqlClient(this, this.isPool());
        return client.incReferenced();
    }

    public abstract Connection createConnection();

    public abstract boolean isPool();

    @Override
    public final boolean execute(String sql) {
        log.debug(sql);
        try (ServerClient client = getClient()) {
            try (Statement st = client.getConnection().createStatement()) {
                st.execute(sql);
                return true;
            }
        } catch (Exception e) {
            log.error("error sql: " + sql);
            throw new RuntimeException(e);
        }
    }

    public final int getTag() {
        return tag;
    }

    public final void setTag(int tag) {
        this.tag = tag;
    }

    protected static final ComboPooledDataSource createDataSource(MysqlConfig config) {
        log.info("create pool to: " + config.getHost());
        // 使用线程池创建
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass(MysqlConfig.JdbcDriver);
        } catch (PropertyVetoException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        dataSource.setJdbcUrl(config.getConnectUrl());
        dataSource.setUser(config.getUser());
        dataSource.setPassword(config.getPassword());
        // 连接池大小设置
        dataSource.setMaxPoolSize(config.getMaxPoolSize());
        dataSource.setMinPoolSize(config.getMinPoolSize());
        dataSource.setInitialPoolSize(config.getInitialPoolSize());
        // 连接池断开控制
        dataSource.setCheckoutTimeout(config.getCheckoutTimeout()); // 单位毫秒
        dataSource.setMaxIdleTime(config.getMaxIdleTime()); // 空闲自动断开时间
        // 每隔多少时间（时间请小于 数据库的 timeout）,测试一下链接，防止失效，会损失小部分性能
        dataSource.setIdleConnectionTestPeriod(config.getIdleConnectionTestPeriod()); // 单位秒
        dataSource.setTestConnectionOnCheckin(true);
        dataSource.setTestConnectionOnCheckout(false);

        return dataSource;
    }

    protected static final HikariDataSource createHikariDataSource(MysqlConfig config) throws SQLException {
// Examines both filesystem and classpath for .properties file
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:mysql://localhost:3306/simpsons");
        hikariConfig.setUsername("bart");
        hikariConfig.setPassword("51mp50n");
        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        dataSource.getMaximumPoolSize();
        dataSource.getConnection();
        return dataSource;
    }

    protected static final Connection getPoolConnection(ComboPooledDataSource dataSource) {
        Connection result = null;
        try {
            result = dataSource.getConnection();
            log.info("MySQL connection pool => getMaxPoolSize {}, connection count {}, busy connection {}",
                    dataSource.getMaxPoolSize(), dataSource.getNumConnections(), dataSource.getNumBusyConnections());
        } catch (SQLException e) {
            if (e.getCause() instanceof InterruptedException)
                log.warn("mysql connection create timeout");
            else if (e.getCause() instanceof TimeoutException)
                log.warn("mysql connection create timeout.");
            else
                log.warn(e.getMessage(), e);
        }
        return result;
    }

    protected final Connection getConnection() {
        return connection;
    }

    protected final void setConnection(Connection connection) {
        this.connection = connection;
    }

    @Override
    public final void close() {
        if (connection != null) {
            try {
                connection.close();
                connection = null;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 取得数据库中所有的表名
     *
     * @return 返回列表
     */
    public final List<String> tables(IHandle handle) {
        if (tables != null)
            return tables;
        tables = new ArrayList<>();
        MysqlQuery query = new MysqlQuery(handle);
        query.add("select TABLE_NAME from information_schema.tables");
        query.add("where table_schema='%s'", this.getDatabase());
        query.open();
        while (query.fetch())
            tables.add(query.getString("TABLE_NAME"));
        return tables;
    }

}
