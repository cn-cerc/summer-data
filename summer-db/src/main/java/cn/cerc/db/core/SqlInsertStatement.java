package cn.cerc.db.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import cn.cerc.core.DataRow;
import cn.cerc.core.FieldDefs;
import cn.cerc.core.FieldMeta;
import cn.cerc.core.FieldMeta.FieldKind;

public class SqlInsertStatement implements AutoCloseable {
    private Connection connection;
    private PreparedStatement statement;
    private StringBuffer sql;
    private int size;

    public SqlInsertStatement(Connection connection) {
        super();
        this.connection = connection;
    }

    public void init(FieldDefs fields, String table) {
        if (sql != null)
            return;

        sql = new StringBuffer();
        sql.append("insert into ").append(table).append(" ");
        sql.append("(");
        size = 0;
        for (FieldMeta meta : fields) {
            if ((meta.getKind() == FieldKind.Storage) && (!meta.isAutoincrement())) {
                if (size++ > 0)
                    sql.append(",");
                sql.append(meta.getCode());
            }
        }
        if (size == 0)
            throw new RuntimeException("not storage field update");

        sql.append(") values (");
        size = 0;
        for (FieldMeta meta : fields) {
            if ((meta.getKind() == FieldKind.Storage) && (!meta.isAutoincrement())) {
                if (size++ > 0)
                    sql.append(",");
                sql.append("?");
            }
        }
        sql.append(")");
        try {
            this.statement = connection.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int saveInsert(DataRow row) throws SQLException {
        int i = 0;
        for (FieldMeta meta : row.fields()) {
            if ((meta.getKind() == FieldKind.Storage) && (!meta.isAutoincrement()))
                statement.setObject(++i, row.getValue(meta.getCode()));
        }
        int result = statement.executeUpdate();
        if (result > 0) {
            ResultSet ids = statement.getGeneratedKeys();
            if (ids.next()) {
                boolean find = false;
                for (FieldMeta meta : row.fields()) {
                    if ((meta.getKind() == FieldKind.Storage) && meta.isAutoincrement()) {
                        if (find)
                            throw new RuntimeException("only support one Autoincrement field");
                        find = true;
                        long uidvalue = ids.getLong(1);
                        if (uidvalue <= Integer.MAX_VALUE) {
                            row.setValue(meta.getCode(), uidvalue);
                        } else {
                            row.setValue(meta.getCode(), uidvalue);
                        }
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void close() {
        try {
            if (statement != null)
                statement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection connection() {
        return connection;
    }

    public PreparedStatement statement() {
        return statement;
    }

    public int size() {
        return size;
    }

    public String sql() {
        return sql.toString();
    }

}
