package cn.cerc.db.core;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Description;

import cn.cerc.db.mssql.MssqlDatabase;
import cn.cerc.db.mysql.MysqlDatabase;
import cn.cerc.db.sqlite.SqliteDatabase;
import cn.cerc.db.testsql.TestsqlServer;

public class EntityHelper<T> {
    private static final Logger log = LoggerFactory.getLogger(EntityHelper.class);

    private static ConcurrentHashMap<Class<?>, EntityHelper<?>> items = new ConcurrentHashMap<>();
    private Class<T> clazz;
    private String tableName;
    private Optional<Field> idField = Optional.empty();
    private Optional<Field> lockedField = Optional.empty();
    private Optional<Field> versionField = Optional.empty();
    private SqlServerType sqlServerType = SqlServerType.Mysql;
    // 找出所有可用的的数据字段
    private Map<String, Field> fields = new LinkedHashMap<>();
    private boolean strict = true;
    private String description;
    private EntityKey entityKey;
    private SqlServer sqlServer;
    private Table table;

    @Deprecated
    public static <T extends EntityImpl> EntityHelper<T> create(Class<T> clazz) {
        return get(clazz);
    }

    public static <T extends EntityImpl> EntityHelper<T> get(Class<T> clazz) {
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

    private EntityHelper(Class<T> class1) {
        super();
        this.clazz = class1;
        this.tableName = class1.getSimpleName();
        for (var clazz : this.getAncestors()) {
            // 查找表名
            Table annoTable = clazz.getDeclaredAnnotation(Table.class);
            if (annoTable != null) {
                this.table = annoTable;
                if (!Utils.isEmpty(annoTable.name()))
                    this.tableName = annoTable.name();
            }

            // 查找数据库类型
            SqlServer annoServer = clazz.getDeclaredAnnotation(SqlServer.class);
            if (annoServer != null) {
                this.sqlServer = annoServer;
                this.sqlServerType = annoServer.type();
                if (TestsqlServer.enabled())
                    this.sqlServerType = SqlServerType.Testsql;
            }

            // 查找是否非严格模式
            Strict annoStrict = clazz.getDeclaredAnnotation(Strict.class);
            if (annoStrict != null)
                this.strict = annoStrict.value();

            // 查找类描述
            Description annoDescription = clazz.getDeclaredAnnotation(Description.class);
            if (annoDescription != null)
                this.description = annoDescription.value();

            EntityKey annoEntityKey = clazz.getDeclaredAnnotation(EntityKey.class);
            if (annoEntityKey != null)
                this.entityKey = annoEntityKey;

            // 查找特殊字段
            for (Field field : clazz.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()))
                    continue;
                Id id = field.getDeclaredAnnotation(Id.class);
                Column column = field.getDeclaredAnnotation(Column.class);
                Version version = field.getDeclaredAnnotation(Version.class);
                if (id == null && column == null && version == null)
                    continue;

                // 开放读取权限
                if (field.getModifiers() == ClassData.DEFAULT || field.getModifiers() == ClassData.PRIVATE
                        || field.getModifiers() == ClassData.PROTECTED)
                    field.setAccessible(true);

                // 查找id标识的字段
                if (id != null) {
                    if (idField.isPresent()) {
                        RuntimeException exception = new RuntimeException("暂不支持多个Id字段");
                        log.error("{} {}", field.toString(), exception.getMessage(), exception);
                        throw exception;
                    }
                    this.idField = Optional.of(field);
                }
                // 查找version标识的字段
                if (version != null) {
                    if (versionField.isPresent())
                        throw new RuntimeException("暂不支持多个Version字段");
                    this.versionField = Optional.of(field);
                }
                var locked = field.getDeclaredAnnotation(Locked.class);
                if (locked != null) {
                    if (lockedField.isPresent())
                        throw new RuntimeException("暂不支持多个Locked字段");
                    this.lockedField = Optional.of(field);
                }
                // 加入字段列表
                fields.put(field.getName(), field);
            }
        }
    }

    /**
     * 返回类的层级，从最上层类到当前类
     */
    private List<Class<?>> getAncestors() {
        List<Class<?>> list = new ArrayList<>();
        putFamily(this.clazz, list);
        List<Class<?>> result = new ArrayList<>();
        for (var i = list.size() - 1; i >= 0; i--)
            result.add(list.get(i));
        return result;
    }

    private static void putFamily(Class<?> clazz, List<Class<?>> list) {
        if (clazz == Object.class || clazz == null)
            return;
        list.add(clazz);
        putFamily(clazz.getSuperclass(), list);
    }

    public Class<T> clazz() {
        return clazz;
    }

    public Table table() {
        return this.table;
    }

    public String tableName() {
        return this.tableName;
    }

    public Optional<Field> idField() {
        return this.idField;
    }

    public Optional<Field> lockedField() {
        return this.lockedField;
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
        return versionField;
    }

    public String versionFieldCode() {
        return versionField.isPresent() ? versionField.get().getName() : null;
    }

    public SqlServerType sqlServerType() {
        return sqlServerType;
    }

    public SqlServer sqlServer() {
        return sqlServer;
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
            log.error(e.getMessage(), e);
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
                        variant.setValue(def).writeToEntity(entity, field);
                }
                if (field.getAnnotation(Version.class) != null)
                    field.set(entity, 0);
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            log.error(e.getMessage(), e);
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
                        variant.setValue(null).writeToEntity(entity, field);
                }
                if (field.getAnnotation(Version.class) != null)
                    field.set(entity, ((Integer) field.get(entity)) + 1);
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public boolean strict() {
        return strict;
    }

    public String description() {
        return description;
    }

    public EntityKey entityKey() {
        return this.entityKey;
    }

    /**
     * 返回与之相关的全部类家族
     */
    public <A extends Annotation> Set<Class<?>> getFamily(Class<A> annotationClass) {
        var result = new HashSet<Class<?>>();
        Class<?> classz = this.clazz;
        if (classz.getSuperclass().isAnnotationPresent(annotationClass))
            classz = classz.getSuperclass();
        if (classz.isAnnotationPresent(annotationClass))
            result.add(classz);
        for (var item : classz.getDeclaredClasses()) {
            if (item.isAnnotationPresent(annotationClass))
                result.add(item);
        }
        return result;
    }
}
