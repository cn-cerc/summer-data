package cn.cerc.db.sqlite;

import java.lang.reflect.InvocationTargetException;

import cn.cerc.core.DataSetGson;
import cn.cerc.core.Utils;
import cn.cerc.db.core.IHandle;

public class SqliteEntity<T> extends SqliteQuery implements IHandle {
    private static final long serialVersionUID = 8276125658457479833L;
    private static SqliteDatabase database;
    private Class<T> clazz;

    public static <U> SqliteEntity<U> Create(Class<U> clazz) {
        if (database == null) {
            database = new SqliteDatabase(clazz);
            database.createTable(false);
        }
        SqliteEntity<U> result = new SqliteEntity<U>(clazz);
        result.operator().setTableName(database.table());
        result.operator().setUpdateKey(database.uid());
        result.add("select * from %s", database.table());
        return result;
    }

    public SqliteEntity(Class<T> clazz) {
        super();
        this.clazz = clazz;
    }

    @Override
    public SqliteEntity<T> open() {
        super.open();
        this.fields().readDefine(clazz);
        return this;
    }

    public T newEntity() {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public SqliteEntity<T> insert(T entity) {
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

    public SqliteEntity<T> update(T entity) {
        this.edit();
        Utils.objectAsRecord(current(), entity);
        return this;
    }

    @Override
    public String json() {
        return new DataSetGson<SqliteEntity<T>>(this).encode();
    }

    @Override
    public SqliteEntity<T> setJson(String json) {
        this.clear();
        if (!Utils.isEmpty(json))
            new DataSetGson<SqliteEntity<T>>(this).decode(json);
        return this;
    }

}
