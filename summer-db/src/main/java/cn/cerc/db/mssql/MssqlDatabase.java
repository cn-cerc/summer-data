package cn.cerc.db.mssql;

import java.lang.reflect.Field;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import cn.cerc.core.Datetime;
import cn.cerc.core.Describe;
import cn.cerc.core.FastDate;
import cn.cerc.core.ISession;
import cn.cerc.core.Utils;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISqlDatabase;

public class MssqlDatabase implements IHandle, ISqlDatabase {
    public static final String DefaultOID = "UpdateKey_";
    private Class<?> clazz;
    private ISession session;

    public MssqlDatabase(IHandle handle, Class<?> clazz) {
        super();
        this.clazz = clazz;
        if (handle != null)
            this.setSession(handle.getSession());
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
        MssqlServer server = (MssqlServer) this.getSession().getProperty(MssqlServer.SessionId);
        List<String> list = server.tables(this);
        String table = table();
        if (!list.contains(table))
            server.execute(getCreateSql());
        return true;
    }

    public String getCreateSql() {
        StringBuffer sb = new StringBuffer();
        sb.append("create table ").append(table()).append(" (");
        int count = 0;
        for (Field field : clazz.getDeclaredFields()) {
            if (count++ > 0)
                sb.append(",");
            sb.append("\n");
            sb.append(field.getName()).append(" ");
            writeDataType(sb, field);
        }
        Table table = clazz.getDeclaredAnnotation(Table.class);
        if (table != null && table.uniqueConstraints().length > 0) {
            sb.append(",\n");
            UniqueConstraint[] uniques = table.uniqueConstraints();
            for (UniqueConstraint unique : uniques) {
                sb.append(String.format("constraint %s primary key clustered (\n", unique.name()));
                count = 0;
                for (String column : unique.columnNames()) {
                    if (count++ > 0)
                        sb.append(",");
                    sb.append(column).append("\n");
                }
                sb.append(
                        ")with (pad_index=off,statistics_norecompute=off,ignore_dup_key=off,allow_row_locks=on,allow_page_locks=on) on [primary]\n");
                sb.append(") on [primary]");
            }
        } else {
            sb.append("\n)");
        }
        if (table != null && table.indexes().length > 0) {
            sb.append("\ngo\n");
            Index[] indexs = table.indexes();
            for (Index index : indexs) {
                sb.append("create");
                if (index.unique()) {
                    sb.append(" unique");
                }
                sb.append(" nonclustered index ").append(index.name()).append(" on ").append(table()).append("(")
                        .append(index.columnList()).append(")").append(" on [primary]").append("\n").append("go\n");
            }
        }
        return sb.toString();
    }

    private void writeDataType(StringBuffer sb, Field field) {
        Column column = field.getDeclaredAnnotation(Column.class);
        if (field.getType() == String.class) {
            int size = 255;
            if (column != null)
                size = column.length();
            if (!Utils.isEmpty(column.columnDefinition())) {
                sb.append(column.columnDefinition());
            } else {
                sb.append("nvarchar(").append(size).append(")");
            }
        } else if (field.getType().isEnum() || field.getType() == Integer.class) {
            if ("tinyint".equals(column.columnDefinition()) || "smallint".equals(column.columnDefinition())) {
                sb.append(column.columnDefinition());
            } else {
                sb.append("int");
            }
        } else if (Datetime.class.isAssignableFrom(field.getType())
                || FastDate.class.isAssignableFrom(field.getType())) {
            sb.append("datetime");
        } else if (field.getType() == Double.class) {
            int precision = 18;
            int scale = 4;
            if (column != null) {
                precision = column.precision();
                scale = column.scale();
            }
            sb.append("numeric(").append(precision).append(",").append(scale).append(")");
        } else if (field.getType() == Float.class) {
            sb.append("float");
        } else if (field.getType() == Boolean.class) {
            sb.append("bit");
        } else {
            throw new RuntimeException("不支持的类型：" + field.getType().getName());
        }
        Describe des = field.getDeclaredAnnotation(Describe.class);
        GeneratedValue gen = field.getDeclaredAnnotation(GeneratedValue.class);
        if (gen != null)
            sb.append(" identity(1,1)");
        if (des != null && !Utils.isEmpty(des.def())) {
            sb.append(" default ").append(des.def());
        }
        if ((column != null) && (!column.nullable()))
            sb.append(" not null");
        else
            sb.append(" null");
    }

    @Override
    public ISession getSession() {
        return session;
    }

    @Override
    public void setSession(ISession session) {
        this.session = session;
    }

}
