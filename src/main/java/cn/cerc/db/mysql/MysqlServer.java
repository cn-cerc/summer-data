package cn.cerc.db.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.resourcepool.TimeoutException;

import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISqlServer;
import cn.cerc.db.core.ServerClient;

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

    protected static final Connection getPoolConnection(ComboPooledDataSource dataSource) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            log.debug("dataSource connection count:" + dataSource.getNumConnections());
        } catch (SQLException e) {
            log.error("jdbc url {}", dataSource.getJdbcUrl());
            if (e.getCause() instanceof InterruptedException)
                log.error(e.getMessage(), e);
            else if (e.getCause() instanceof TimeoutException)
                log.error(e.getMessage(), e);
            else
                log.error(e.getMessage(), e);
        }
        return connection;
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
