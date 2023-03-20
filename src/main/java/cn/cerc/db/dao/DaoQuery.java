package cn.cerc.db.dao;

import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.EntityImpl;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlText;
import cn.cerc.db.core.Utils;
import cn.cerc.db.mysql.MysqlQuery;

public class DaoQuery<T extends EntityImpl> extends MysqlQuery {
    private static final long serialVersionUID = -4833075222571787291L;
    private Class<T> clazz;

    public DaoQuery(IHandle handle, Class<T> clazz) {
        super(handle);
        this.clazz = clazz;
        this.setSql(new SqlText(this.clazz));
    }

    @Deprecated
    public final void append(T item) {
        insert(item);
    }

    @Deprecated
    public final void save(T item) {
        update(item);
    }

    public T currentEntity() {
        return this.asEntity(clazz).orElseThrow();
    }

    @Deprecated
    public final T read() {
        return this.asEntity(clazz).orElseThrow();
    }

    @Override
    public String json() {
        return new DataSetGson<DaoQuery<T>>(this).encode();
    }

    @Override
    public DaoQuery<T> setJson(String json) {
        this.clear();
        if (!Utils.isEmpty(json))
            new DataSetGson<DaoQuery<T>>(this).decode(json);
        return this;
    }

}
