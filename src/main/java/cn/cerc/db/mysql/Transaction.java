package cn.cerc.db.mysql;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.IHandle;
import cn.cerc.db.testsql.TestsqlServer;

public class Transaction implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Transaction.class);

    private MysqlClient client;
    private boolean active = false;
    private boolean locked = false;

    public Transaction(IHandle owner) {
        if (TestsqlServer.enabled())
            return;
        MysqlServerMaster mysql = owner.getMysql();
        this.client = mysql.getClient();
        try {
            Connection connection = client.getConnection();
            if (connection.getAutoCommit()) {
                connection.setAutoCommit(false);
                this.active = true;
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public boolean commit() {
        if (TestsqlServer.enabled())
            return true;
        if (!active)
            return false;

        if (locked)
            throw new RuntimeException("Transaction locked is true");

        try {
            client.getConnection().commit();
            locked = true;
            return true;
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (TestsqlServer.enabled())
            return;
        try {
            if (active) {
                Connection connection = client.getConnection();
                try {
                    connection.rollback();
                } finally {
                    connection.setAutoCommit(true);
                }
            }
            client.close();
        } catch (SQLException e) {
            client.close();
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public boolean isActive() {
        return active;
    }
}
