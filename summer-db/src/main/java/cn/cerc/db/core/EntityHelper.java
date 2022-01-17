package cn.cerc.db.core;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;

import cn.cerc.db.mssql.MssqlDatabase;
import cn.cerc.db.mysql.MysqlDatabase;
import cn.cerc.db.sqlite.SqliteDatabase;

public class EntityHelper<T> {
    private static ConcurrentHashMap<Class<?>, EntityHelper<?>> items = new ConcurrentHashMap<>();
    private static final int PRIVATE = 2;
    private static final int PROTECTED = 4;
    private Class<T> clazz;
    private String table;
    private Optional<Field> idField = Optional.empty();
    private Optional<Field> versionField = Optional.empty();
    private SqlServerType sqlServerType = SqlServerType.Mysql;
    // 找出所有可用的的数据字段
    private Map<String, Field> fields = new LinkedHashMap<>();

    public static <T> EntityHelper<T> create(Class<T> clazz) {
        @SuppressWarnings("unchecked")
        EntityHelper<T> result = (EntityHelper<T>) items.get(clazz);
        if (result != null)
            return result;
        synchronized (EntityHelper.class) {
            result = new EntityHelper<T>(clazz);
            items.put(clazz, result);
            return result;
        }
    }

    private EntityHelper(Class<T> clazz) {
        super();
        this.clazz = clazz;
        // 查找表名
        Table object = clazz.getDeclaredAnnotation(Table.class);
        if (object != null && !Utils.isEmpty(object.name()))
            this.table = object.name();
        else
            this.table = clazz.getSimpleName();
        // 查找数据库类型
        SqlServer server = clazz.getAnnotation(SqlServer.class);
        if (server != null)
            this.sqlServerType = server.type();

        // 查找特殊字段
        for (Field field : clazz.getDeclaredFields()) {
            Id id = field.getDeclaredAnnotation(Id.class);
            Column column = field.getDeclaredAnnotation(Column.class);
            Version version = field.getDeclaredAnnotation(Version.class);
            if (id == null && column == null && version == null)
                continue;

            // 开放读取权限
            if (field.getModifiers() == PRIVATE || field.getModifiers() == PROTECTED)
                field.setAccessible(true);

            // 查找id标识的字段
            if (id != null) {
                if (idField.isPresent())
                    throw new RuntimeException("暂不支持多个Id字段");
                this.idField = Optional.of(field);
            }
            // 查找version标识的字段
            if (version != null) {
                if (versionField.isPresent())
                    throw new RuntimeException("暂不支持多个Version字段");
                this.versionField = Optional.of(field);
            }
            // 加入字段列表
            String name = field.getName();
            if (column != null && !"".equals(column.name()))
                name = column.name();
            fields.put(name, field);
        }
    }

    public Class<T> clazz() {
        return clazz;
    }

    public String table() {
        return this.table;
    }

    public Optional<Field> idField() {
        return this.idField;
    }

    public String idFieldCode() {
        if (idField.isPresent())
            return idField.get().getName();
        switch (sqlServerType) {
        case Mysql:
            return MysqlDatabase.DefaultOID;
        case Mssql:
            return MssqlDatabase.DefaultOID;
        default:
            return SqliteDatabase.DefaultOID;
        }
    }

    public Optional<Field> versionField() {
        return idField;
    }

    public String versionFieldCode() {
        return idField.isPresent() ? idField.get().getName() : null;
    }

    public SqlServerType sqlServerType() {
        return sqlServerType;
    }

    public T newEntity() {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public Object readIdValue(EntityImpl entity) {
        try {
            return idField.get().get(entity);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    public Map<String, Field> fields() {
        return this.fields;
    }

    public void onInsertPostDefault(EntityImpl entity) {
        Variant variant = new Variant();
        try {
            for (Field field : fields.values()) {
                Column column = field.getAnnotation(Column.class);
                if ((column != null && field.get(entity) == null)) {
                    Describe describe = field.getAnnotation(Describe.class);
                    String def = describe != null ? describe.def() : null;
                    if (!column.nullable() || !Utils.isEmpty(def))
                        variant.setData(def).writeToEntity(entity, field);
                }
                if (field.getAnnotation(Version.class) != null)
                    field.setInt(entity, 0);
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void onUpdatePostDefault(EntityImpl entity) {
        Map<String, Field> items = this.fields;
        Variant variant = new Variant();
        try {
            for (Field field : items.values()) {
                Column column = field.getAnnotation(Column.class);
                if ((column != null && field.get(entity) == null)) {
                    if (!column.nullable())
                        variant.setData(null).writeToEntity(entity, field);
                }
                if (field.getAnnotation(Version.class) != null)
                    field.setInt(entity, field.getInt(entity) + 1);
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
