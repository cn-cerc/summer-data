package cn.cerc.db.mssql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import cn.cerc.db.core.IConfig;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISqlServer;
import cn.cerc.db.core.ServerConfig;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MssqlServer implements ISqlServer, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MssqlServer.class);

    // 数据库连接
    public static final String MSSQL_SITE = "mssql.site";
    // 数据库端口
    public static final String MSSQL_PORT = "mssql.port";
    // 数据库名称
    public static final String MSSQL_DATABASE = "mssql.database";
    // 数据库用户
    public static final String MSSQL_USERNAME = "mssql.username";
    // 数据库密码
    public static final String MSSQL_PASSWORD = "mssql.password";

    // ISession 中识别码
    public static final String SessionId = "mssqlSession";

    private String url;
    private String user;
    private String password;
    private Connection connection;
    private String database;

    private IConfig config;

    private List<String> tables;

    private String host;

    public MssqlServer() {
        config = ServerConfig.getInstance();
        database = config.getProperty(MssqlServer.MSSQL_DATABASE, "appdb");
    }

    @Override
    public void close() throws Exception {
        try {
            if (connection != null) {
                log.info("close mssql connection");
                connection.close();
                connection = null;
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    public final Connection getConnection() {
        if (connection != null) {
            return connection;
        }

        try {
            if (url == null) {
                url = getConnectUrl();
            }

            log.debug("mssql url {} ", url);
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            connection = DriverManager.getConnection(url, user, password);
            return connection;
        } catch (SQLException | ClassNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

    }

    public final String getConnectUrl() {
        String site = config.getProperty(MssqlServer.MSSQL_SITE, "127.0.0.1");
        String port = config.getProperty(MssqlServer.MSSQL_PORT, "1433");
        this.host = site + ":" + port;

        user = config.getProperty(MssqlServer.MSSQL_USERNAME, "appdb_user");
        password = config.getProperty(MssqlServer.MSSQL_PASSWORD, "appdb_password");

        if (site == null || database == null || user == null || password == null) {
            throw new RuntimeException("mssql connection error.");
        }

        // jdbc:sqlserver://112.124.37.146:1433;databaseName=MIMRC_Std;
        return String.format("jdbc:sqlserver://%s:%s;databaseName=%s", site, port, database);
    }

    @Override
    public final boolean execute(String sql) {
        log.debug(sql);
        try {
            Statement st = getConnection().createStatement();
            st.execute(sql);
            return true;
        } catch (SQLException e) {
            log.error("error mssql: {}", sql);
            throw new RuntimeException(e);
        }
    }

    @Override
    public MssqlClient getClient() {
        return new MssqlClient(this.getConnection());
    }

    public String getDatabase() {
        return database;
    }

    /**
     * 
     * @param handle handle
     * @return 取得数据库中所有的表名
     */
    public final List<String> tables(IHandle handle) {
        if (tables != null)
            return tables;
        tables = new ArrayList<>();
        MssqlQuery query = new MssqlQuery(handle);
        query.add("select name from sys.tables where type='U'");
        query.open();
        while (query.fetch())
            tables.add(query.getString("name"));
        return tables;
    }

    @Override
    public String getHost() {
        return host;
    }

}
