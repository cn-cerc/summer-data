package cn.cerc.core;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Id;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityUtils {
    private static final Logger log = LoggerFactory.getLogger(EntityUtils.class);
    private static int PUBLIC = 1;
    private static int PRIVATE = 2;
    private static int PROTECTED = 4;

    public static Map<String, Field> getFields(Class<?> entityClass) {
        // 找出所有可用的的数据字段
        Map<String, Field> items = new LinkedHashMap<>();
        for (Field field : entityClass.getDeclaredFields()) {
            Column column = field.getAnnotation(Column.class);
            if (column != null) {
                String name = !"".equals(column.name()) ? column.name() : field.getName();
                if (field.getModifiers() == EntityUtils.PRIVATE || field.getModifiers() == EntityUtils.PROTECTED) {
                    field.setAccessible(true);
                    items.put(name, field);
                } else if (field.getModifiers() == PUBLIC) {
                    items.put(name, field);
                }
                continue;
            }
            Id id = field.getAnnotation(Id.class);
            if (id != null) {
                if (field.getModifiers() == PRIVATE || field.getModifiers() == PROTECTED
                        || field.getModifiers() == PUBLIC)
                    items.put(field.getName(), field);
            }
        }
        return items;
    }

    // 将record的数据，复制到obj中
    @SuppressWarnings("deprecation")
    public static void copyToEntity(DataRow record, Object entity)
            throws IllegalArgumentException, IllegalAccessException {
        Map<String, Field> items = getFields(entity.getClass());
        if (record.fields().size() > items.size()) {
            log.warn("fields.size > propertys.size");
        } else if (record.fields().size() < items.size()) {
            String fmt = "fields.size %d < propertys.size %d";
            throw new RuntimeException(String.format(fmt, record.fields().size(), items.size()));
        }

        // 查找并赋值
        for (FieldMeta meta : record.fields()) {
            Object value = record.getValue(meta.code());

            // 查找指定的对象属性
            Field field = null;
            for (String itemName : items.keySet()) {
                if (itemName.equals(meta.code())) {
                    field = items.get(itemName);
                    if (field.getModifiers() == PRIVATE || field.getModifiers() == PROTECTED)
                        field.setAccessible(true);
                    break;
                }
            }
            if (field == null) {
                log.warn("not find property: " + meta.code());
                continue;
            }

            // 给属性赋值
            if (value == null) {
                field.set(entity, null);
            } else if (field.getType().equals(value.getClass())) {
                field.set(entity, value);
            } else {
                if ("int".equals(field.getType().getName())) {
                    field.setInt(entity, (Integer) value);
                } else if ("double".equals(field.getType().getName())) {
                    field.setDouble(entity, (Double) value);
                } else if ("long".equals(field.getType().getName())) {
                    if (value instanceof BigInteger) {
                        field.setLong(entity, ((BigInteger) value).longValue());
                    } else {
                        field.setLong(entity, (Long) value);
                    }
                } else if ("boolean".equals(field.getType().getName())) {
                    field.setBoolean(entity, (Boolean) value);
                } else if (TDateTime.class.getName().equals(field.getType().getName())) {
                    field.set(entity, new TDateTime((Date) value));
                } else if (Datetime.class.getName().equals(field.getType().getName())) {
                    if (value instanceof String)
                        field.set(entity, new Datetime((String) value));
                    else if (value instanceof Date)
                        field.set(entity, new Datetime((Date) value));
                    else
                        throw new RuntimeException(String.format("field %s error: %s as %s", field.getName(),
                                value.getClass().getName(), field.getType().getName()));
                } else if (field.getType().isEnum()) {
                    int tmp = 0;
                    if (value instanceof Double)
                        tmp = ((Double) value).intValue();
                    else if (value instanceof Integer)
                        tmp = ((Integer) value).intValue();
                    else if (value != null)
                        throw new RuntimeException("not support type:" + value.getClass());
                    @SuppressWarnings({ "unchecked", "rawtypes" })
                    Class<Enum> clazz = (Class<Enum>) field.getType();
                    @SuppressWarnings("rawtypes")
                    Enum[] list = clazz.getEnumConstants();
                    if (tmp >= 0 && tmp < list.length)
                        field.set(entity, list[tmp]);
                    else
                        throw new RuntimeException(String.format("error enum %d of %s", tmp, clazz.getName()));
                } else if (value.getClass() == Double.class && field.getType() == Integer.class) {
                    Double tmp = (Double) value;
                    field.set(entity, tmp.intValue());
                } else {
                    throw new RuntimeException(String.format("field %s error: %s as %s", field.getName(),
                            value.getClass().getName(), field.getType().getName()));
                }
            }
        }
    }

}
