package cn.cerc.db.mysql;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlOperator;
import cn.cerc.db.core.SqlText;

public class MysqlOperator extends SqlOperator {
    private static final Logger log = LoggerFactory.getLogger(MysqlOperator.class);
    private final IHandle handle;

    public MysqlOperator(IHandle handle) {
        super();
        this.setUpdateKey("UID_");
        this.handle = handle;
    }

    @Deprecated
    public static String findTableName(String sql) {
        return SqlText.findTableName(sql);
    }

    @Deprecated
    public boolean insert(DataRow record) {
        try (MysqlClient client = handle.getMysql().getClient()) {
            return insert(client.getConnection(), record);
        }
    }

    @Deprecated
    public boolean update(DataRow record) {
        try (MysqlClient client = handle.getMysql().getClient()) {
            return update(client.getConnection(), record);
        }
    }

    @Deprecated
    public boolean delete(DataRow record) {
        try (MysqlClient client = handle.getMysql().getClient()) {
            return delete(client.getConnection(), record);
        }
    }

    @Override
    protected BigInteger findAutoUid(Connection conn) {
        BigInteger result = null;
        String sql = "SELECT LAST_INSERT_ID() ";
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
    protected String getKeyByDB(Connection conn, String tableName) throws SQLException {
        StringBuffer result = new StringBuffer();
        try (BuildStatement bs = new BuildStatement(conn)) {
            bs.append("select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS ");
            bs.append("where table_name= ? AND COLUMN_KEY= 'PRI' ", tableName);
            PreparedStatement ps = bs.build();
            log.debug(ps.toString().split(":")[1].trim());
            ResultSet rs = ps.executeQuery();
            int i = 0;
            while (rs.next()) {
                i++;
                if (i > 1) {
                    result.append(";");
                }
                result.append(rs.getString("COLUMN_NAME"));
            }
            return result.toString();
        }
    }

}
