package cn.cerc.db.pgsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISqlServer;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.mssql.MssqlQuery;

public class PgsqlServer implements ISqlServer, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PgsqlServer.class);
    // 数据库连接
    public static final String PGSQL_SITE = "pgsql.site";
    // 数据库端口
    public static final String PGSQL_PORT = "pgsql.port";
    // 数据库名称
    public static final String PGSQL_DATABASE = "pgsql.database";
    // 数据库用户
    public static final String PGSQL_USERNAME = "pgsql.username";
    // 数据库密码
    public static final String PGSQL_PASSWORD = "pgsql.password";
    // 线程池最大联接数
    public static final String MaxConnections = "pgsql.connections.max";
    // ISession 中识别码
    public static final String SessionId = "pgsqlSession";

//    private String driver = "org.postgresql.Driver";
    private static ServerConfig config;
    private static String pqsql_host;
    private Connection connection;
    private List<String> tables;

    static {
        config = ServerConfig.getInstance();
        String site = config.getProperty(PgsqlServer.PGSQL_SITE, "127.0.0.1");
        String port = config.getProperty(PgsqlServer.PGSQL_PORT, "5432");
        pqsql_host = site + ":" + port;
    }

    @Override
    public PgsqlClient getClient() {
        return new PgsqlClient(this.getConnection());
    }

    public Connection getConnection() {
        if (connection != null)
            return connection;

        String dbname = config.getProperty(PgsqlServer.PGSQL_DATABASE, "postgres");
        String username = config.getProperty(PgsqlServer.PGSQL_USERNAME);
        String password = config.getProperty(PgsqlServer.PGSQL_PASSWORD);
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e1) {
            log.error(e1.getMessage());
            e1.printStackTrace();
            return null;
        }
        String url = String.format("jdbc:postgresql://%s/%s", this.getHost(), dbname);
        try {
            log.info("{}, {}, {}", url, username, password);
            connection = DriverManager.getConnection(url, username, password);
            return connection;
        } catch (SQLException e1) {
            log.error(e1.getMessage());
            e1.printStackTrace();
            return null;
        }
    }

    @Override
    public void close() {
        try {
            if (connection != null) {
                log.info("close pgsql connection");
                connection.close();
                connection = null;
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public boolean execute(String sql) {
        log.debug(sql);
        try {
            Connection conn = Objects.requireNonNull(getConnection());
            Statement st = conn.createStatement();
            st.execute(sql);
            return true;
        } catch (SQLException e) {
            log.error("error mssql: {}", sql);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getHost() {
        return pqsql_host;
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

    public static void main(String[] args) {
        try (PgsqlServer pg = new PgsqlServer()) {
            pg.execute("select * from table1");
        }
    }
}
