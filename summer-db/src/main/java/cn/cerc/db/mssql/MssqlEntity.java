package cn.cerc.db.mssql;

import java.lang.reflect.InvocationTargetException;

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
        result.operator().setTableName(database.table());
        result.add("select * from %s", database.table());
        return result;
    }

    public MssqlEntity(IHandle handle, Class<T> clazz) {
        super(handle);
        this.clazz = clazz;
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
        return current().asObject(clazz);
    }

    public T currentEntity() {
        return current().asObject(clazz);
    }

    public MssqlEntity<T> update(T entity) {
        this.edit();
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
