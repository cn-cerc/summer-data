package cn.cerc.db.testsql;

import java.sql.Connection;

import cn.cerc.db.core.ServerClient;

public class TestsqlClient implements ServerClient {

    @Override
    public void close() throws Exception {

    }

    @Override
    public Connection getConnection() {
        return null;
    }

}
