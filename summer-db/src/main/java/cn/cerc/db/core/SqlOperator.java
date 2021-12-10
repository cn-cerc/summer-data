package cn.cerc.db.core;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.core.ClassResource;
import cn.cerc.core.DataRow;
import cn.cerc.core.FieldMeta;
import cn.cerc.core.FieldMeta.FieldKind;
import cn.cerc.core.Utils;
import cn.cerc.db.SummerDB;
import cn.cerc.db.mssql.MssqlDatabase;
import cn.cerc.db.mysql.BuildStatement;
import cn.cerc.db.mysql.MysqlDatabase;
import cn.cerc.db.mysql.UpdateMode;
import cn.cerc.db.sqlite.SqliteDatabase;

public abstract class SqlOperator {
    private static final ClassResource res = new ClassResource(SqlOperator.class, SummerDB.ID);
    private static final Logger log = LoggerFactory.getLogger(SqlOperator.class);
    private String table;
    private String oid;
    private UpdateMode updateMode = UpdateMode.strict;
    private boolean debug = false;
    private List<String> searchKeys = new ArrayList<>();

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

    public final boolean debug() {
        return debug;
    }

    @Deprecated
    public final boolean isDebug() {
        return debug();
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

    public final boolean insert(Connection connection, DataRow record) {
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
                    if (i > 1) {
                        bs.append(",");
                    }
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
                        if (i == 1) {
                            bs.append("?", record.getValue(field));
                        } else {
                            bs.append(",?", record.getValue(field));
                        }
                    }
                }
            }
            bs.append(")");

            if (i == 0)
                throw new RuntimeException("not storage field update");

            lastCommand = bs.getPrepareCommand();
            log.debug(bs.getPrepareCommand());
            PreparedStatement ps = bs.build();
            if (this.debug()) {
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
            log.error(lastCommand);
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    public final boolean update(Connection connection, DataRow record) {
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
            if (i == 0)
                throw new RuntimeException("no field is update");

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

            if (this.updateMode() == UpdateMode.strict) {
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
            if (this.debug()) {
                log.info(bs.getPrepareCommand());
                return false;
            }

            if (ps.executeUpdate() != 1) {
                log.error(bs.getPrepareCommand());
                throw new RuntimeException(res.getString(1, "当前记录已被其它用户修改或不存在，更新失败"));
            }
            return true;
        } catch (SQLException e) {
            log.error(lastCommand);
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    public final boolean delete(Connection connection, DataRow record) {
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
            if (this.debug()) {
                log.info(bs.getPrepareCommand());
                return false;
            }
            return ps.execute();
        } catch (SQLException e) {
            log.error(lastCommand);
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

    public final List<String> getSearchKeys() {
        return searchKeys;
    }

    public final void setSearchKeys(List<String> searchKeys) {
        this.searchKeys = searchKeys;
    }

    @Deprecated // 请改使用 getSearchKeys
    public final List<String> getPrimaryKeys() {
        return searchKeys;
    }

    // 从数据库中获取主键
    protected abstract String getKeyByDB(Connection connection, String tableName2) throws SQLException;

    // 取得自动增加栏位的最新值
    protected abstract BigInteger findAutoUid(Connection connection);

}
