package cn.cerc.db.core;

import java.sql.Connection;

public interface ISqlClient extends AutoCloseable {

    Connection getConnection();

}
