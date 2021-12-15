package cn.cerc.db.mysql;

import java.lang.reflect.Field;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import cn.cerc.core.Datetime;
import cn.cerc.core.Describe;
import cn.cerc.core.FastDate;
import cn.cerc.core.ISession;
import cn.cerc.core.Utils;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISqlDatabase;

public class MysqlDatabase implements IHandle, ISqlDatabase {
    public static final String DefaultOID = "UID_";
    private Class<?> clazz;
    private ISession session;

    public MysqlDatabase(IHandle handle, Class<?> clazz) {
        super();
        this.clazz = clazz;
        if (handle != null)
            this.setSession(handle.getSession());
    }

    @Override
    public final String table() {
        return Utils.findTable(clazz);
    }

    @Override
    public String oid() {
        return Utils.findOid(clazz, DefaultOID);
    }

    @Override
    public boolean createTable(boolean overwrite) {
        MysqlServerMaster server = this.getMysql();
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
        if (table != null && table.indexes().length > 0) {
            sb.append(",");
            Index[] indexs = table.indexes();
            count = 0;
            for (Index index : indexs) {
                if (count++ > 0) {
                    sb.append(",");
                }
                sb.append("\n");
                if ("PRIMARY".equals(index.name())) {
                    sb.append("primary key (").append(index.columnList()).append(")");
                    continue;
                }
                if (index.unique()) {
                    sb.append("unique ");
                }
                sb.append("index ").append(index.name()).append(" (").append(index.columnList()).append(")");
            }
        } else {
            String fields = "";
            for (Field field : clazz.getDeclaredFields()) {
                Id id = field.getDeclaredAnnotation(Id.class);
                if (id != null) {
                    fields = fields + field.getName() + ",";
                }
            }
            if (Utils.isEmpty(fields))
                throw new RuntimeException("lack primary key");
            sb.append(",").append("\n").append("primary key (").append(fields.substring(0, fields.length() - 1))
                    .append(")");
        }
        sb.append("\n) engine=innodb charset=utf8 collate=utf8_general_ci;");
        return sb.toString();
    }

    private void writeDataType(StringBuffer sb, Field field) {
        Column column = field.getDeclaredAnnotation(Column.class);
        if (field.getType() == String.class) {
            int size = 255;
            if (column != null)
                size = column.length();
            if (!Utils.isEmpty(column.columnDefinition()) && "text".equals(column.columnDefinition())) {
                sb.append("text");
            } else {
                sb.append("varchar(").append(size).append(")");
            }
        } else if (field.getType().isEnum() || field.getType() == int.class || field.getType() == long.class) {
            int size = 11;
            if (column != null)
                size = column.length();
            sb.append("int(").append(size).append(")");
        } else if (Datetime.class.isAssignableFrom(field.getType())
                || FastDate.class.isAssignableFrom(field.getType())) {
            sb.append("datetime");
        } else if (field.getType() == double.class) {
            int precision = 18;
            int scale = 4;
            if (column != null) {
                precision = column.precision();
                scale = column.scale();
            }
            sb.append("decimal(").append(precision).append(",").append(scale).append(")");
        } else if (field.getType() == boolean.class) {
            sb.append("bit(").append(1).append(")");
        } else {
            throw new RuntimeException("不支持的类型：" + field.getType().getName());
        }
        Describe des = field.getDeclaredAnnotation(Describe.class);
        if (des != null && !Utils.isEmpty(des.def())) {
            sb.append(" default ").append(des.def());
        }
        Id id = field.getDeclaredAnnotation(Id.class);
        if ((column != null) && (!column.nullable()))
            sb.append(" not null");
        else if (id != null)
            sb.append(" not null");
        else
            sb.append(" null");
        GeneratedValue gen = field.getDeclaredAnnotation(GeneratedValue.class);
        if (gen != null) {
            sb.append(" auto_increment");
        }
        if (des != null) {
            sb.append(" comment '").append(Utils.isEmpty(des.name()) ? field.getName() : des.name()).append("'");
        }
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
