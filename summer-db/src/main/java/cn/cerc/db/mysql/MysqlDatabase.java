package cn.cerc.db.mysql;

import java.lang.reflect.Field;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import cn.cerc.core.Datetime;
import cn.cerc.core.ISession;
import cn.cerc.core.Utils;
import cn.cerc.db.core.IHandle;

public class MysqlDatabase implements IHandle {
    private Class<?> clazz;
    private ISession session;

    public MysqlDatabase(IHandle handle, Class<?> clazz) {
        super();
        this.clazz = clazz;
        if (handle != null)
            this.setSession(handle.getSession());
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
        MysqlServerMaster server = this.getMysql();
        List<String> list = server.tables(this);
        String table = table();
        if (!list.contains(table))
            server.execute(getCreateSql());
        return true;
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
        else
            sb.append(" default null");
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
