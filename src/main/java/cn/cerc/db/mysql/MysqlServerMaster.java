package cn.cerc.db.mysql;

import java.sql.Connection;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.mchange.v2.c3p0.ComboPooledDataSource;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MysqlServerMaster extends MysqlServer {
    // IHandle中识别码
    public static final String SessionId = "sqlSession";
    private static ComboPooledDataSource dataSource;

    static {
        ZkMysqlConfig config = ZkMysqlConfig.getMaster();
        if (config.maxPoolSize() > 0)
            dataSource = config.createDataSource();
    }

    @Override
    public Connection createConnection() {
        if (isPool()) // 使用线程池创建
            return MysqlServer.getPoolConnection(dataSource);

        // 不使用线程池直接创建
        if (getConnection() == null) 
            setConnection(ZkMysqlConfig.getMaster().createConnection());
        return getConnection();
    }

    @Override
    public final boolean isPool() {
        return dataSource != null;
    }

    @Override
    public String getHost() {
        return ZkMysqlConfig.getMaster().site();
    }

    @Override
    public String getDatabase() {
        return ZkMysqlConfig.getMaster().database();
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
