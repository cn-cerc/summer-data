package cn.cerc.db.dao;

import java.lang.reflect.ParameterizedType;

import cn.cerc.core.DataSetGson;
import cn.cerc.core.RecordUtils;
import cn.cerc.core.SqlText;
import cn.cerc.core.Utils;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.mysql.MysqlQuery;

public class DaoQuery<T> extends MysqlQuery {
    private static final long serialVersionUID = -4833075222571787291L;
    private Class<T> clazz;

    @SuppressWarnings("unchecked")
    public DaoQuery(IHandle handle) {
        super(handle);
        ParameterizedType ptype = (ParameterizedType) getClass().getGenericSuperclass();
        this.clazz = (Class<T>) ptype.getActualTypeArguments()[0];
        this.setSqlText(new SqlText(this.clazz));
    }

    public DaoQuery(IHandle handle, Class<T> clazz) {
        super(handle);
        this.clazz = clazz;
        this.setSqlText(new SqlText(this.clazz));
    }

    // 将对象追加到数据表中
    public void append(T item) {
        if (item instanceof DaoEvent) {
            ((DaoEvent) item).beforePost();
        }
        this.append();
        RecordUtils.copyToRecord(item, this.current());
        this.post();
    }

    // 与read函数配套，将对象内容保存到数据库中
    public void save(T item) {
        if (item instanceof DaoEvent) {
            ((DaoEvent) item).beforePost();
        }
        this.edit();
        RecordUtils.copyToRecord(item, this.current());
        this.post();
    }

    public T read() {
        return this.current().asObject(clazz);
    }

    @Override
    public String json() {
        return new DataSetGson<DaoQuery<T>>(this).encode();
    }

    @Override
    public DaoQuery<T> setJson(String json) {
        this.close();
        if (!Utils.isEmpty(json))
            new DataSetGson<DaoQuery<T>>(this).decode(json);
        return this;
    }

}
