package cn.cerc.db.mysql;

import java.sql.Connection;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.zaxxer.hikari.HikariDataSource;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MysqlServerSlave extends MysqlServer {
    // IHandle中识别码
    public static final String SessionId = "slaveSqlSession";
    private static HikariDataSource dataSource;

    static {
        var config = MysqlConfig.getSlave();
        dataSource = config.createDataSource();
    }

    @Override
    public Connection createConnection() {
        // 使用线程池创建
        if (isPool())
            return MysqlServer.getPoolConnection(dataSource);

        // 直接创建连接
        if (getConnection() == null)
            setConnection(MysqlConfig.getMaster().createConnection());
        return this.getConnection();
    }

    @Override
    public boolean isPool() {
        return true;
    }

    @Override
    public String getHost() {
        return MysqlConfig.getSlave().site();
    }

    @Override
    public String getDatabase() {
        return MysqlConfig.getSlave().database();
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
