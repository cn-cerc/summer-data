package cn.cerc.core;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Column;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordUtils {
    private static final Logger log = LoggerFactory.getLogger(RecordUtils.class);
    private static int PUBLIC = 1;
    private static int PRIVATE = 2;
    private static int PROTECTED = 4;

    // 将obj的数据，复制到record中
    public static void copyToRecord(Object obj, DataRow record) {
        try {
            Field[] fields = obj.getClass().getDeclaredFields();
            for (Field field : fields) {
                Column column = findColumn(field.getAnnotations());
                if (column != null) {
                    String fieldName = field.getName();
                    if (!"".equals(column.name())) {
                        fieldName = column.name();
                    }
                    if (field.getModifiers() == PUBLIC) {
                        record.setValue(fieldName, field.get(obj));
                    } else if (field.getModifiers() == PRIVATE || field.getModifiers() == PROTECTED) {
                        field.setAccessible(true);
                        record.setValue(fieldName, field.get(obj));
                    }
                }
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    // 将record的数据，复制到obj中
    @SuppressWarnings("deprecation")
    public static void copyToObject(DataRow record, Object obj) {
        try {
            Field[] fields = obj.getClass().getDeclaredFields();
            // 找出所有的数据字段
            Map<Field, Column> items = new HashMap<>();
            for (Field field : fields) {
                Column column = findColumn(field.getAnnotations());
                if (column != null) {
                    if (field.getModifiers() == PUBLIC) {
                        items.put(field, column);
                    } else if (field.getModifiers() == PRIVATE || field.getModifiers() == PROTECTED) {
                        field.setAccessible(true);
                        items.put(field, column);
                    }
                }
            }

            if (record.fields().size() != items.size()) {
                if (record.fields().size() > items.size()) {
                    log.warn("field[].size > property[].size");
                } else {
                    throw new RuntimeException(String.format("field[].size %d < property[].size %d",
                            record.fields().size(), items.size()));
                }
            }

            // 查找并赋值
            for (String fieldName : record.fields().names()) {
                boolean exists = false;
                for (Field field : items.keySet()) {
                    // 默认等于对象的属性
                    String propertyName = field.getName();
                    Column column = items.get(field);
                    if (!"".equals(column.name())) {
                        propertyName = column.name();
                    }
                    if (propertyName.equals(fieldName)) {
                        Object val = record.getValue(fieldName);
                        if (val == null) {
                            field.set(obj, null);
                        } else if (field.getType().equals(val.getClass())) {
                            field.set(obj, val);
                        } else {
                            if ("int".equals(field.getType().getName())) {
                                field.setInt(obj, (Integer) val);
                            } else if ("double".equals(field.getType().getName())) {
                                field.setDouble(obj, (Double) val);
                            } else if ("long".equals(field.getType().getName())) {
                                if (val instanceof BigInteger) {
                                    field.setLong(obj, ((BigInteger) val).longValue());
                                } else {
                                    field.setLong(obj, (Long) val);
                                }
                            } else if ("boolean".equals(field.getType().getName())) {
                                field.setBoolean(obj, (Boolean) val);
                            } else if (TDateTime.class.getName().equals(field.getType().getName())) {
                                field.set(obj, new TDateTime((Date) val));
                            } else if (Datetime.class.getName().equals(field.getType().getName())) {
                                field.set(obj, new Datetime((Date) val));
                            } else {
                                throw new RuntimeException("error: " + field.getType().getName() + " as " + val.getClass().getName());
                            }
                        }
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    log.warn("property not find: " + fieldName);
                }
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    private static Column findColumn(Annotation[] annotations) {
        Column column = null;
        for (Annotation item : annotations) {
            if (item instanceof Column) {
                column = (Column) item;
                break;
            }
        }
        return column;
    }

}
