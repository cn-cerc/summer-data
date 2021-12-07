package cn.cerc.db.mssql;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import cn.cerc.core.DataRow;
import cn.cerc.core.SqlText;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlOperator;

public class MssqlOperator extends SqlOperator {
    private final IHandle handle;

    public MssqlOperator(IHandle handle) {
        super();
        this.setUpdateKey(MssqlDatabase.DefaultUID);
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
    protected BigInteger findAutoUid(Connection conn) {
        BigInteger result = null;
//        String sql = "SELECT SCOPE_IDENTITY()";
        String sql = "SELECT @@identity";
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            if (rs.next()) {
                Object obj = rs.getObject(1);
                if (obj instanceof BigInteger) {
                    result = (BigInteger) obj;
                } else {
                    result = BigInteger.valueOf(rs.getInt(1));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
        if (result == null) {
            throw new RuntimeException("未获取UID");
        }
        return result;
    }

    @Override
    protected String getKeyByDB(Connection connection, String tableName) throws SQLException {
        return null;
    }

}
