package cn.cerc.db.sqlite;

import java.lang.reflect.Field;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import cn.cerc.core.Datetime;
import cn.cerc.core.Utils;

public class SqliteDatabase {
    private Class<?> clazz;

    public SqliteDatabase(Class<?> clazz) {
        super();
        this.clazz = clazz;
    }

    public final String table() {
        String table = null;
        // 取得表名
        Table object = this.clazz.getDeclaredAnnotation(Table.class);
        if (object != null)
            table = object.name();
        if (Utils.isEmpty(table))
            table = this.clazz.getSimpleName();
        return table;
    }

    public boolean createTable(boolean overwrite) {
        SqliteServer server = new SqliteServer();
        return server.createTable(getCreateSql(), overwrite);
    }

    public String getCreateSql() {
        StringBuffer sb = new StringBuffer();
        sb.append("create table ").append(table()).append("(");
        int count = 0;
        for (Field field : clazz.getDeclaredFields()) {
            if (count++ > 0)
                sb.append(",");
            sb.append("\n");
            sb.append(field.getName()).append(" ");
            writeDataType(sb, field);
        }
        sb.append("\n)");
        return sb.toString();
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
        } else if (field.getType() == int.class) {
            sb.append("INTEGER");
        } else if (field.getType() == double.class) {
            sb.append("float");
        } else {
            throw new RuntimeException("不支持的类型：" + field.getType().getName());
        }
        Id id = field.getDeclaredAnnotation(Id.class);
        if (id != null) {
            sb.append(" primary key");
        }
        GeneratedValue gen = field.getDeclaredAnnotation(GeneratedValue.class);
        if (gen != null) {
            sb.append(" AUTOINCREMENT");
        }
        if ((column != null) && (!column.nullable()))
            sb.append(" not null");
        else if (id != null)
            sb.append(" not null");
        else
            sb.append(" default null");
    }

}
