package cn.cerc.db.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import cn.cerc.db.core.Utils;
import cn.cerc.db.zk.ZkConfig;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MysqlServerMaster extends MysqlServer {
    // IHandle中识别码
    public static final String SessionId = "sqlSession";
    private static ComboPooledDataSource dataSource;
    private static final ZkConfig config = new ZkConfig("/mysql");

    static {
        if (config.getInt(MysqlConfig.rds_MaxPoolSize, 0) > 0)
            dataSource = MysqlServer.createDataSource(config);
    }

    @Override
    public Connection createConnection() {
        if (isPool()) // 使用线程池创建
            return MysqlServer.getPoolConnection(dataSource);

        try {
            // 不使用线程池直接创建
            if (getConnection() == null) {
                var host = config.getString(MysqlConfig.rds_site);
                var database = config.getString(MysqlConfig.rds_database);
                var timezone = config.getString(MysqlConfig.rds_ServerTimezone);
                if (Utils.isEmpty(host) || Utils.isEmpty(database) || Utils.isEmpty(timezone))
                    throw new RuntimeException("mysql connection config is null");
                var jdbcUrl = String.format(
                        "jdbc:mysql://%s/%s?useSSL=false&autoReconnect=true&autoCommit=false&useUnicode=true&characterEncoding=utf8&serverTimezone=%s",
                        host, database, timezone);

                Class.forName(MysqlConfig.JdbcDriver);
                setConnection(DriverManager.getConnection(jdbcUrl, config.getString(MysqlConfig.rds_username),
                        config.getString(MysqlConfig.rds_password)));
            }
            return getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e.getCause());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public final boolean isPool() {
        return dataSource != null;
    }

    @Override
    public String getHost() {
        return config.getString(MysqlConfig.rds_site);
    }

    @Override
    public String getDatabase() {
        return config.getString(MysqlConfig.rds_database);
    }

    public static void openPool() {

    }

    public static void closePool() {
        if (dataSource != null) {
            dataSource.close();
            dataSource = null;
        }
    }

}
