package cn.cerc.db.sqlite;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import cn.cerc.db.core.SqlOperator;

public class SqliteOperator extends SqlOperator {

    public SqliteOperator() {
        super();
        this.setUpdateKey("id_");
    }

    @Override
    protected BigInteger findAutoUid(Connection conn) {
        BigInteger result = null;
        String sql = "select last_insert_rowid() newid";
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
    protected String getKeyByDB(Connection connection, String tableName2) throws SQLException {
        return null;
    }

}
