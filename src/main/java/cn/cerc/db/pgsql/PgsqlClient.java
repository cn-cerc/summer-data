package cn.cerc.db.pgsql;

import java.sql.Connection;

import cn.cerc.db.core.ServerClient;

public class PgsqlClient implements ServerClient {
    private final Connection connection;

    public PgsqlClient(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public void close() {

    }

}