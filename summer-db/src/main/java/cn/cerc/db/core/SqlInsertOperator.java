package cn.cerc.db.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlInsertOperator implements AutoCloseable {
    private PreparedStatement statement;
    private Connection connection;
    private StringBuffer sql;
    private int size;

    public SqlInsertOperator(Connection connection, FieldDefs fields, String table) {
        super();
        this.connection = connection;
        sql = new StringBuffer();
        sql.append("insert into ").append(table).append(" ");
        sql.append("(");
        size = 0;
        for (FieldMeta meta : fields) {
            if (meta.insertable()) {
                if (size++ > 0)
                    sql.append(",");
                sql.append(meta.code());
            }
        }
        if (size == 0)
            throw new RuntimeException("not storage field update");

        sql.append(") values (");
        size = 0;
        for (FieldMeta meta : fields) {
            if (meta.insertable()) {
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

    public int save(DataRow row) throws SQLException {
        int i = 0;
        for (FieldMeta meta : row.fields()) {
            if (meta.insertable())
                statement.setObject(++i, row.getValue(meta.code()));
        }
        int result = statement.executeUpdate();
        if (result > 0) {
            FieldMeta meta = row.fields().getByAutoincrement();
            if (meta != null) {
                ResultSet ids = statement.getGeneratedKeys();
                if (ids.next()) {
                    long uidvalue = ids.getLong(1);
                    if (uidvalue <= Integer.MAX_VALUE) {
                        row.setValue(meta.code(), uidvalue);
                    } else {
                        row.setValue(meta.code(), uidvalue);
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
