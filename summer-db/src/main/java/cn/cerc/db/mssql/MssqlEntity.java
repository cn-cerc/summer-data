package cn.cerc.db.mssql;

import java.lang.reflect.InvocationTargetException;

import cn.cerc.core.DataRow;
import cn.cerc.core.DataSetGson;
import cn.cerc.core.Utils;
import cn.cerc.db.core.IHandle;

public class MssqlEntity<T> extends MssqlQuery implements IHandle {
    private static final long serialVersionUID = 8276125658457479833L;
    private static MssqlDatabase database;
    private Class<T> clazz;

    public static <U> MssqlEntity<U> Create(IHandle handle, Class<U> clazz) {
        if (database == null) {
            database = new MssqlDatabase(handle, clazz);
            database.createTable(false);
        }
        MssqlEntity<U> result = new MssqlEntity<U>(handle, clazz);
        result.operator().setTable(database.table());
        result.operator().setOid(database.oid());
        result.add("select * from %s", database.table());
        return result;
    }

    public MssqlEntity(IHandle handle, Class<T> clazz) {
        super(handle);
        this.clazz = clazz;
    }

    @Override
    public MssqlEntity<T> open() {
        super.open();
        this.fields().readDefine(clazz);
        return this;
    }

    public T findOne(Object key) {
        this.add("where %s='%s'", database.oid(), key);
        return this.currentEntity();
    }

    public T currentEntity() {
        DataRow row = current();
        if (row == null)
            return null;
        return row.asEntity(clazz);
    }

    public T newEntity() {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public MssqlEntity<T> insert(T entity) {
        this.append();
        Utils.objectAsRecord(current(), entity);
        return this;
    }

    public T editEntity() {
        edit();
        return current().asEntity(clazz);
    }

    public MssqlEntity<T> update(T entity) {
        Utils.objectAsRecord(current(), entity);
        return this;
    }

    @Override
    public String json() {
        return new DataSetGson<MssqlEntity<T>>(this).encode();
    }

    @Override
    public MssqlEntity<T> setJson(String json) {
        this.clear();
        if (!Utils.isEmpty(json))
            new DataSetGson<MssqlEntity<T>>(this).decode(json);
        return this;
    }

}
