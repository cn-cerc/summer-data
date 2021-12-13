package cn.cerc.db.dao;

import cn.cerc.core.DataSetGson;
import cn.cerc.core.RecordUtils;
import cn.cerc.core.SqlText;
import cn.cerc.core.Utils;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.mysql.MysqlQuery;

public class DaoQuery<T> extends MysqlQuery {
    private static final long serialVersionUID = -4833075222571787291L;
    private Class<T> clazz;

    public DaoQuery(IHandle handle, Class<T> clazz) {
        super(handle);
        this.clazz = clazz;
        this.setSql(new SqlText(this.clazz));
    }

    // 将对象追加到数据表中
    public void insert(T item) {
        if (item instanceof DaoEvent) {
            ((DaoEvent) item).beforePost();
        }
        this.append();
        RecordUtils.copyToRecord(item, this.current());
        this.post();
    }

    @Deprecated
    public final void append(T item) {
        insert(item);
    }
    
    public void update(T item) {
        if (item instanceof DaoEvent) {
            ((DaoEvent) item).beforePost();
        }
        this.edit();
        RecordUtils.copyToRecord(item, this.current());
        this.post();
    }
    
    @Deprecated
    public final void save(T item) {
        update(item);
    }

    public T currentEntity() {
        return this.current().asEntity(clazz);
    }

    @Deprecated
    public final T read() {
        return currentEntity();
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
