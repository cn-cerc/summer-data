package cn.cerc.db.core;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.SummerDB;
import cn.cerc.db.core.FieldMeta.FieldKind;
import cn.cerc.db.mssql.MssqlClient;
import cn.cerc.db.mssql.MssqlDatabase;
import cn.cerc.db.mssql.MssqlServer;
import cn.cerc.db.mysql.BuildStatement;
import cn.cerc.db.mysql.MysqlClient;
import cn.cerc.db.mysql.MysqlDatabase;
import cn.cerc.db.mysql.MysqlServerMaster;
import cn.cerc.db.pgsql.PgsqlDatabase;
import cn.cerc.db.sqlite.SqliteDatabase;

public class SqlOperator implements IHandle {
    private static final ClassResource res = new ClassResource(SqlOperator.class, SummerDB.ID);
    private static final Logger log = LoggerFactory.getLogger(SqlOperator.class);
    private SqlServerType sqlServerType;
    private String table;
    private String oid;
    private UpdateMode updateMode = UpdateMode.strict;
    private boolean debug = false;
    private List<String> searchKeys = new ArrayList<>();
    private ISession session;
    private String versionField;

    public SqlOperator(IHandle handle, SqlServerType sqlServerType) {
        super();
        if (handle != null)
            this.session = handle.getSession();
        this.sqlServerType = sqlServerType;
        switch (sqlServerType) {
        case Mysql, Testsql:
            this.setOid(MysqlDatabase.DefaultOID);
            break;
        case Mssql:
            this.setOid(MssqlDatabase.DefaultOID);
            break;
        case Sqlite:
            this.setOid(SqliteDatabase.DefaultOID);
            break;
        case Pgsql:
            this.setOid(PgsqlDatabase.DefaultOID);
            break;
        default:
            throw new SqlServerTypeException();
        }
    }

    public final String table() {
        return table;
    }

    @Deprecated
    public final String getTableName() {
        return table();
    }

    public final SqlOperator setTable(String table) {
        this.table = table;
        return this;
    }

    @Deprecated
    public final void setTableName(String tableName) {
        this.setTable(tableName);
    }

    public final String oid() {
        return this.oid;
    }

    @Deprecated
    public final String getUpdateKey() {
        return oid();
    }

    public final SqlOperator setOid(String oid) {
        this.oid = oid;
        return this;
    }

    @Deprecated
    public final void setUpdateKey(String updateKey) {
        this.setOid(updateKey);
    }

    public final UpdateMode updateMode() {
        return updateMode;
    }

    @Deprecated
    public final UpdateMode getUpdateMode() {
        return updateMode();
    }

    public final SqlOperator setUpdateMode(UpdateMode updateMode) {
        this.updateMode = updateMode;
        return this;
    }

    public final boolean isDebug() {
        return debug;
    }

    public final void setDeubg(boolean debug) {
        this.debug = debug;
    }

    @Deprecated // 请改使用 oid
    public final String getPrimaryKey() {
        return oid();
    }

    @Deprecated // 请改使用 setUpdateKey
    public final void setPrimaryKey(String primaryKey) {
        this.setOid(primaryKey);
    }

    // 取出所有数据
    public int select(DataSet dataSet, Connection connection, String sql) throws SQLException {
        int total = 0;
        try (Statement st = connection.createStatement()) {
            try (ResultSet rs = st.executeQuery(sql.replace("\\", "\\\\"))) {
                // 取得字段清单
                ResultSetMetaData meta = rs.getMetaData();
                FieldDefs defs = dataSet.fields();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    String field = meta.getColumnLabel(i);
                    if (!defs.exists(field))
                        defs.add(field, FieldKind.Storage);
                }
                // 取得所有内容
                while (rs.next()) {
                    DataRow row = dataSet.createDataRow();
                    if (row == null)
                        break;
                    total++;
                    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                        String fn = rs.getMetaData().getColumnLabel(i);
                        row.setValue(fn, rs.getObject(fn));
                    }
                    row.setState(DataRowState.None);
                }
            }
        }
        return total;
    }

    public boolean insert(Connection connection, DataRow record) {
        resetUpdateKey(connection, record);

        if (MssqlDatabase.DefaultOID.equalsIgnoreCase(this.oid)) {
            if ("".equals(record.getString(this.oid)))
                record.setValue(this.oid, Utils.newGuid());
        }

        String lastCommand = null;
        try (BuildStatement bs = new BuildStatement(connection)) {
            bs.append("insert into ").append(this.table()).append(" (");
            int i = 0;
            for (String field : record.items().keySet()) {
                FieldMeta meta = record.fields().get(field);
                if (meta.storage() && !meta.autoincrement()) {
                    i++;
                    if (i > 1)
                        bs.append(",");
                    bs.append(field);
                }
            }
            bs.append(") values (");
            i = 0;
            for (String field : record.items().keySet()) {
                FieldMeta meta = record.fields().get(field);
                if (meta.kind() == FieldKind.Storage) {
                    if (!meta.autoincrement()) {
                        i++;
                        if (i > 1)
                            bs.append(",");
                        bs.append("?", record.getValue(meta.code()));
                    }
                }
            }
            bs.append(")");

            if (i == 0)
                throw new RuntimeException("not storage field update");

            lastCommand = bs.getPrepareCommand();
            log.debug(bs.getPrepareCommand());
            PreparedStatement ps = bs.build();
            if (this.debug) {
                log.info(bs.getPrepareCommand());
                return false;
            }

            int result = ps.executeUpdate();

            boolean find = false;
            for (FieldMeta meta : record.fields()) {
                if (meta.storage() && meta.autoincrement()) {
                    if (find)
                        throw new RuntimeException("only support one Autoincrement field");
                    find = true;
                    BigInteger uidvalue = findAutoUid(connection);
                    if (uidvalue == null)
                        throw new RuntimeException("uid value is null");
                    log.debug("自增列uid value：" + uidvalue);
                    if (uidvalue.intValue() <= Integer.MAX_VALUE) {
                        record.setValue(meta.code(), uidvalue.intValue());
                    } else {
                        record.setValue(meta.code(), uidvalue);
                    }
                }
            }

            return result > 0;
        } catch (SQLException e) {
            log.error(lastCommand, e);
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    public boolean update(Connection connection, DataRow record) {
        Map<String, Object> delta = record.delta();
        if (delta.size() == 0)
            return true;

        resetUpdateKey(connection, record);

        String lastCommand = null;
        try (BuildStatement bs = new BuildStatement(connection)) {
            bs.append("update ").append(this.table());
            // 加入set条件
            int i = 0;
            for (String field : delta.keySet()) {
                FieldMeta meta = record.fields().get(field);
                if (meta.storage()) {
                    if (!meta.autoincrement()) {
                        i++;
                        bs.append(i == 1 ? " set " : ",");
                        bs.append(field);
                        if (field.indexOf("+") >= 0 || field.indexOf("-") >= 0) {
                            bs.append("?", record.getValue(field));
                        } else {
                            bs.append("=?", record.getValue(field));
                        }
                    }
                }
            }
            if (i == 0) {
                log.error("update table {} error，record is {}", this.table(), record);
                throw new RuntimeException("no field is update");
            }

            // 加入 where 条件
            i = 0;
            int pkCount = 0;
            for (String field : searchKeys) {
                FieldMeta meta = record.fields().get(field);
                if (meta.kind() == FieldKind.Storage) {
                    i++;
                    bs.append(i == 1 ? " where " : " and ").append(field);
                    Object value = delta.containsKey(field) ? delta.get(field) : record.getValue(field);
                    if (value != null) {
                        bs.append("=?", value);
                        pkCount++;
                    } else {
                        throw new RuntimeException("serachKey not is null: " + field);
                    }
                }
            }
            if (pkCount == 0)
                throw new RuntimeException("serach keys value not exists");

            if (versionField != null) {
                bs.append(" and ").append(versionField);
                bs.append("=?", delta.get(versionField));
            } else if (this.updateMode() == UpdateMode.strict) {
                for (String field : delta.keySet()) {
                    if (!searchKeys.contains(field)) {
                        FieldMeta meta = record.fields().get(field);
                        if (meta.kind() == FieldKind.Storage) {
                            i++;
                            bs.append(i == 1 ? " where " : " and ").append(field);
                            Object value = delta.get(field);
                            if (value != null) {
                                bs.append("=?", value);
                            } else {
                                bs.append(" is null ");
                            }
                        }
                    }
                }
            }

            lastCommand = bs.getPrepareCommand();
            log.debug(bs.getPrepareCommand());
            PreparedStatement ps = bs.build();
            if (this.debug) {
                log.info(bs.getPrepareCommand());
                return false;
            }

            if (ps.executeUpdate() != 1) {
                RuntimeException e = new RuntimeException(res.getString(1, "当前记录已被其它用户修改或不存在，更新失败"));
                log.error(bs.getPrepareCommand(), e);
                throw e;
            }
            return true;
        } catch (SQLException e) {
            log.error(lastCommand, e);
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    public boolean delete(Connection connection, DataRow record) {
        resetUpdateKey(connection, record);

        String lastCommand = null;
        try (BuildStatement bs = new BuildStatement(connection)) {
            bs.append("delete from ").append(this.table());
            int count = 0;
            Map<String, Object> delta = record.delta();
            for (String field : searchKeys) {
                FieldMeta meta = record.fields().get(field);
                if (meta.storage()) {
                    Object value = delta.containsKey(field) ? delta.get(field) : record.getValue(field);
                    if (value == null)
                        throw new RuntimeException("primary key is null");
                    count++;
                    bs.append(count == 1 ? " where " : " and ");
                    bs.append(field).append("=? ", value);
                }
            }

            if (count == 0)
                throw new RuntimeException("serach keys value not exists");

            lastCommand = bs.getPrepareCommand();
            log.debug(bs.getPrepareCommand());
            PreparedStatement ps = bs.build();
            if (this.debug) {
                log.info(bs.getPrepareCommand());
                return false;
            }
            return ps.execute();
        } catch (SQLException e) {
            log.error(lastCommand, e);
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    private void resetUpdateKey(Connection connection, DataRow record) {
        FieldMeta def = null;
        if (!Utils.isEmpty(this.oid)) {
            def = record.fields().get(this.oid);
            if (def == null) {
                log.debug(String.format("not find primary key %s in %s", this.oid(), this.table()));
                this.oid = null;
            }
        }

        if (def != null && def.kind() == FieldKind.Storage) {
            def.setIdentification(true);
            if (MysqlDatabase.DefaultOID.equals(this.oid) || SqliteDatabase.DefaultOID.equals(this.oid)
                    || "id".equals(this.oid)) {
                def.setAutoincrement(true);
                def.setInsertable(false);
            }
        }

        boolean find = false;
        for (FieldMeta meta : record.fields()) {
            if (meta.identification()) {
                if (find)
                    throw new RuntimeException("only support one UpdateKey field");
                find = true;
                this.oid = meta.code();
                if (!searchKeys.contains(oid))
                    searchKeys.add(oid);
            }
        }

        if (this.searchKeys.size() == 0) {
            try {
                String result = getKeyByDB(connection, this.table());
                if (!Utils.isEmpty(result)) {
                    String[] pks = result.split(";");
                    if (pks.length == 0)
                        throw new RuntimeException("获取不到主键PK");
                    for (String pk : pks) {
                        this.searchKeys.add(pk);
                        break;
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e.getMessage());
            }
        }

        if (searchKeys.size() == 0)
            throw new RuntimeException("search key is empty");
    }

    @Deprecated // 请改使用 getSearchKeys
    public final List<String> getPrimaryKeys() {
        return searchKeys;
    }

    // 从数据库中获取主键
    protected String getKeyByDB(Connection connection, String tableName) throws SQLException {
        if (sqlServerType != SqlServerType.Mysql)
            return null;

        StringBuffer result = new StringBuffer();
        try (BuildStatement bs = new BuildStatement(connection)) {
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

    // 取得自动增加栏位的最新值
    protected BigInteger findAutoUid(Connection conn) {
        BigInteger result = null;

        String sql = null;
        switch (sqlServerType) {
        case Mysql:
            sql = "SELECT LAST_INSERT_ID()";
            break;
        case Mssql:
            sql = "SELECT @@identity";
            break;
        case Sqlite:
            sql = "select last_insert_rowid() newid";
            break;
        case Pgsql:
            sql = String.format("SELECT currval('%s_%s_seq')", this.table, this.oid);
            break;
        default:
            throw new SqlServerTypeException();
        }
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
    public ISession getSession() {
        return session;
    }

    @Override
    public void setSession(ISession session) {
        this.session = session;
    }

    @Deprecated
    public static String findTableName(String sql) {
        return SqlText.findTableName(sql);
    }

    @Deprecated
    public boolean insert(DataRow record) {
        if (sqlServerType == SqlServerType.Mssql) {
            MssqlServer server = (MssqlServer) session.getProperty(MssqlServer.SessionId);
            try (MssqlClient client = server.getClient()) {
                return insert(client.getConnection(), record);
            }
        } else if (sqlServerType == SqlServerType.Mysql) {
            MysqlServerMaster server = (MysqlServerMaster) session.getProperty(MysqlServerMaster.SessionId);
            try (MysqlClient client = server.getClient()) {
                return insert(client.getConnection(), record);
            }
        } else {
            return false;
        }
    }

    @Deprecated
    public boolean update(DataRow record) {
        if (sqlServerType == SqlServerType.Mssql) {
            MssqlServer server = (MssqlServer) session.getProperty(MssqlServer.SessionId);
            try (MssqlClient client = server.getClient()) {
                return update(client.getConnection(), record);
            }
        } else if (sqlServerType == SqlServerType.Mysql) {
            MysqlServerMaster server = (MysqlServerMaster) session.getProperty(MysqlServerMaster.SessionId);
            try (MysqlClient client = server.getClient()) {
                return update(client.getConnection(), record);
            }
        } else {
            return false;
        }
    }

    @Deprecated
    public boolean delete(DataRow record) {
        if (sqlServerType == SqlServerType.Mssql) {
            MssqlServer server = (MssqlServer) session.getProperty(MssqlServer.SessionId);
            try (MssqlClient client = server.getClient()) {
                return delete(client.getConnection(), record);
            }
        } else if (sqlServerType == SqlServerType.Mysql) {
            MysqlServerMaster server = (MysqlServerMaster) session.getProperty(MysqlServerMaster.SessionId);
            try (MysqlClient client = server.getClient()) {
                return delete(client.getConnection(), record);
            }
        } else {
            return false;
        }
    }

    public String getVersionField() {
        return versionField;
    }

    public void setVersionField(String versionField) {
        this.versionField = versionField;
    }

}
