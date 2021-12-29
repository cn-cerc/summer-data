package cn.cerc.db.core;

import java.sql.Connection;

public interface ServerClient extends AutoCloseable {

    Connection getConnection();

}
