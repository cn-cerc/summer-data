package cn.cerc.db.sqlite;

import java.lang.reflect.Field;

import javax.persistence.Column;

import cn.cerc.core.Datetime;

public class SqliteTableCreate {
    private StringBuffer sb;

    public SqliteTableCreate(Class<?> clazz, String table) {
        super();
        sb = new StringBuffer();
        sb.append("create table ").append(table).append("(");
        int count = 0;
        for (Field field : clazz.getDeclaredFields()) {
            if (count++ > 0)
                sb.append(",");
            sb.append("\n");
            sb.append(field.getName()).append(" ");
            writeDataType(sb, field);
        }
        sb.append("\n)");
    }

    private void writeDataType(StringBuffer sb, Field field) {
        Column column = field.getDeclaredAnnotation(Column.class);
        if (field.getType() == String.class) {
            int size = 255;
            if (column != null)
                size = column.length();
            sb.append("varchar(").append(size).append(")");
        } else if (field.getType().isEnum()) {
            sb.append("int");
        } else if (Datetime.class.isAssignableFrom(field.getType())) {
            sb.append("datetime");
        } else {
            throw new RuntimeException("不支持的类型：" + field.getType().getName());
        }
        if (column != null) {
            if (!column.nullable())
                sb.append(" not null");
        }
    }

    public boolean execute(boolean overwrite) {
        SqliteServer server = new SqliteServer();
        return server.createTable(sqlText(), overwrite);
    }

    public String sqlText() {
        return sb.toString();
    }

    public SqliteTableCreate setSqlText(String value) {
        sb.setLength(0);
        sb.append(value);
        return this;
    }
}
