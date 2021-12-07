package cn.cerc.db.mssql;

import java.lang.reflect.Field;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import cn.cerc.core.Datetime;
import cn.cerc.core.ISession;
import cn.cerc.core.Utils;
import cn.cerc.db.core.IHandle;

public class MssqlDatabase implements IHandle {
    public static final String DefaultUID = "UpdateKey_";
    private Class<?> clazz;
    private ISession session;

    public MssqlDatabase(IHandle handle, Class<?> clazz) {
        super();
        this.clazz = clazz;
        if (handle != null)
            this.setSession(handle.getSession());
    }

    public final String table() {
        return Utils.findTable(this.clazz);
    }

    public final String uid() {
        return Utils.findUid(clazz, DefaultUID);
    }
    
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
            sb.append("int");
        } else if (field.getType() == double.class) {
            sb.append("float");
        } else {
            throw new RuntimeException("不支持的类型：" + field.getType().getName());
        }
        Id id = field.getDeclaredAnnotation(Id.class);
        if (id != null)
            sb.append(" primary key");
        GeneratedValue gen = field.getDeclaredAnnotation(GeneratedValue.class);
        if (gen != null)
            sb.append(" identity");
        if ((id == null) && (column != null) && (!column.nullable()))
            sb.append(" not null");
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
