package cn.cerc.db.mssql;

import java.sql.Connection;

import cn.cerc.db.core.ServerClient;

public class MssqlClient implements ServerClient {
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
