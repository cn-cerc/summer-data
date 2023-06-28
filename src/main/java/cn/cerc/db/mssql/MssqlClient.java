package cn.cerc.db.mssql;

import java.sql.Connection;

import cn.cerc.db.core.ISqlClient;

public class MssqlClient implements ISqlClient {
    private final Connection connection;

    public MssqlClient(Connection connection) {
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
