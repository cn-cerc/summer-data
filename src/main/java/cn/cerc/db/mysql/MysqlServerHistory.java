package cn.cerc.db.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlServerHistory extends MysqlServer {
    private static final Logger log = LoggerFactory.getLogger(MysqlServerHistory.class);

    public MysqlServerHistory(String database) {
        super();
    }

    @Override
    public String getHost() {
        return ZkMysqlConfig.getMaster().site();
    }

    @Override
    public String getDatabase() {
        return ZkMysqlConfig.getMaster().database();
    }

    @Override
    public Connection createConnection() {
        // 不使用线程池直接创建
        try {
            var config = ZkMysqlConfig.getMaster();
            if (getConnection() == null) {
                Class.forName(ZkMysqlConfig.JdbcDriver);
                setConnection(
                        DriverManager.getConnection(config.getConnectUrl(), config.username(), config.password()));
            }
            return getConnection();
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isPool() {
        return false;
    }

}
