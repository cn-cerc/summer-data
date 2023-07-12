package cn.cerc.db.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import cn.cerc.db.core.ServerClient;

public class MysqlClient implements ServerClient {
    private AtomicInteger count = new AtomicInteger();
    private final MysqlServer mysql;
    private Connection connection;
    private boolean pool;

    public MysqlClient(MysqlServer mysql, boolean isPool) {
        this.mysql = mysql;
        this.pool = isPool;
    }

    public MysqlClient incReferenced() {
        if (pool) {
            count.incrementAndGet();
//            System.out.println("referenced count(create)= " + count);
        }
        return this;
    }

    @Override
    public void close() {
        if (pool) {
            if (count.decrementAndGet() == 0) {
                try {
                    if (connection != null) {
                        connection.close();
                        connection = null;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
//            System.out.println("referenced count(close) = " + count);
        }
    }

    @Override
    public final Connection getConnection() {
        if (connection == null) {
            synchronized (this) {
                if (connection == null)
                    this.connection = mysql.createConnection();
            }
        }
        return connection;
    }

    public boolean isPool() {
        return pool;
    }

}
