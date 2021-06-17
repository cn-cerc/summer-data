package cn.cerc.core;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

public class ClassData {
    public static final int PUBLIC = 1;
    public static final int PRIVATE = 2;
    public static final int PROTECTED = 4;
    private Class<?> clazz;
    private String tableId = null;
    private String select = null;
    private Map<String, Field> fields = null;
    private Field generationIdentityField = null;
    private String updateKey = "UID_";
    private List<String> searchKeys = new ArrayList<>();
    private List<String> specialNumKeys = new ArrayList<>();

    public ClassData(Class<?> clazz) {
        this.clazz = clazz;
        for (Annotation anno : clazz.getAnnotations()) {
            if (anno instanceof Entity) {
                Entity entity = (Entity) anno;
                tableId = entity.name();
            }
        }

        this.fields = loadFields();

        if (!Utils.isEmpty(tableId)) {
            StringBuffer sb = new StringBuffer();
            sb.append("select ");
            int i = 0;
            for (String key : fields.keySet()) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append("`" + key + "`");
                i++;
            }
            sb.append(" from ").append("`" + tableId + "`");
            select = sb.toString();
        }
        
        // 查找自增字段并赋值
        int count = 0;
        for (String key : fields.keySet()) {
            Field field = fields.get(key);
            for (Annotation item : field.getAnnotations()) {
                if (item instanceof GeneratedValue) {
                    if (((GeneratedValue) item).strategy() == GenerationType.IDENTITY) {
                        generationIdentityField = field;
                        count++;
                    }
                }
                if (item instanceof Id) {
                    updateKey = key;
                }
                if (item instanceof SearchKey) {
                    searchKeys.add(key);
                }
                if (item instanceof SpecialNum) {
                    specialNumKeys.add(key);
                }
            }
        }

        if (count > 1) {
            throw new RuntimeException("support one generationIdentityField!");
        }

        if (searchKeys.size() == 0) {
            searchKeys.add(updateKey);
        }
    }

    private Map<String, Field> loadFields() {
        Map<String, Field> fields = new LinkedHashMap<>();
        for (Field field : clazz.getDeclaredFields()) {
            Column column = null;
            for (Annotation item : field.getAnnotations()) {
                if (item instanceof Column) {
                    column = (Column) item;
                    break;
                }
            }
            if (column != null) {
                String fieldCode = field.getName();
                if (!"".equals(column.name())) {
                    fieldCode = column.name();
                }

                if (field.getModifiers() == Modifier.PUBLIC) {
                    fields.put(fieldCode, field);
                } else if (field.getModifiers() == Modifier.PRIVATE || field.getModifiers() == Modifier.PROTECTED) {
                    field.setAccessible(true);
                    fields.put(fieldCode, field);
                }
            }
        }
        return fields;
    }

    public List<String> getSearchKeys() {
        return searchKeys;
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public String getTableId() {
        return tableId;
    }

    public String getSelect() {
        return select;
    }

    public Map<String, Field> getFields() {
        return fields;
    }

    public Field getGenerationIdentityField() {
        return generationIdentityField;
    }

    public String getUpdateKey() {
        return updateKey;
    }

    public List<String> getSpecialNumKeys() {
        return specialNumKeys;
    }
}
