package cn.cerc.db.sqlite;

import java.lang.reflect.Field;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import cn.cerc.core.Datetime;
import cn.cerc.core.Utils;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISqlDatabase;

public class SqliteDatabase implements ISqlDatabase {
    public static final String DefaultOID = "id_";
    private Class<?> clazz;

    public SqliteDatabase(IHandle handle, Class<?> clazz) {
        super();
        this.clazz = clazz;
    }

    @Override
    public final String table() {
        return Utils.findTable(this.clazz);
    }

    @Override
    public final String oid() {
        return Utils.findOid(clazz, DefaultOID);
    }

    @Override
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
