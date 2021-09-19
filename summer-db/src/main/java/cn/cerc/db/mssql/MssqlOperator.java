package cn.cerc.db.mssql;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;

import cn.cerc.core.DataRow;
import cn.cerc.core.SqlText;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlOperator;

public class MssqlOperator extends SqlOperator {
    private final IHandle handle;

    public MssqlOperator(IHandle handle) {
        super();
        this.setUpdateKey("UpdateKey_");
        this.handle = handle;
    }

    @Deprecated
    public static String findTableName(String sql) {
        return SqlText.findTableName(sql);
    }

    @Deprecated
    public boolean insert(DataRow record) {
        MssqlServer server = (MssqlServer) handle.getSession().getProperty(MssqlServer.SessionId);
        try (MssqlClient client = server.getClient()) {
            return insert(client.getConnection(), record);
        }
    }

    @Deprecated
    public boolean update(DataRow record) {
        MssqlServer server = (MssqlServer) handle.getSession().getProperty(MssqlServer.SessionId);
        try (MssqlClient client = server.getClient()) {
            return update(client.getConnection(), record);
        }
    }

    @Deprecated
    public boolean delete(DataRow record) {
        MssqlServer server = (MssqlServer) handle.getSession().getProperty(MssqlServer.SessionId);
        try (MssqlClient client = server.getClient()) {
            return delete(client.getConnection(), record);
        }
    }

    @Override
    protected BigInteger findAutoUid(Connection connection) {
        return null;
    }

    @Override
    protected String getKeyByDB(Connection connection, String tableName) throws SQLException {
        return null;
    }

}
