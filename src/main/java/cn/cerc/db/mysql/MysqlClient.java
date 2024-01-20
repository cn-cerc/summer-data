package cn.cerc.db.mysql;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.ServerClient;

public class MysqlClient implements ServerClient {
    private static final Logger log = LoggerFactory.getLogger(MysqlClient.class);

    private int count = 0;
    private final MysqlServer mysql;
    private Connection connection;
    private boolean pool;

    public MysqlClient(MysqlServer mysql, boolean isPool) {
        this.mysql = mysql;
        this.pool = isPool;
    }

    public MysqlClient incReferenced() {
        if (pool) {
            ++count;
//            System.out.println("referenced count(create)= " + count);
        }
        return this;
    }

    @Override
    public void close() {
        if (pool) {
            if (--count == 0) {
                try {
                    if (connection != null) {
                        connection.close();
                        connection = null;
                    }
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }
            }
//            System.out.println("referenced count(close) = " + count);
        }
    }

    @Override
    public final Connection getConnection() {
        if (connection == null) {
            if (!mysql.isPool()) {
                pool = false;
                count = 0;
            }
            this.connection = mysql.createConnection();
        }
        return connection;
    }

    public boolean isPool() {
        return pool;
    }

}
