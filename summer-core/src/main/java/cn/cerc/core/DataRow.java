package cn.cerc.core;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Id;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;

public class DataRow implements Serializable, IRecord {
    private static final Logger log = LoggerFactory.getLogger(DataRow.class);
    private static final long serialVersionUID = 4454304132898734723L;
    private static final int PUBLIC = 1;
    private static final int PRIVATE = 2;
    private static final int PROTECTED = 4;
    private DataRowState state = DataRowState.None;
    private Map<String, Object> items = new LinkedHashMap<>();
    private DataSet dataSet;
    private FieldDefs fields;
    private boolean createFields;
    private DataRow history;

    public DataRow() {
        super();
        this.fields = new FieldDefs();
        this.createFields = true;
    }

    public DataRow(DataSet dataSet) {
        super();
        this.dataSet = dataSet;
        this.fields = dataSet.fields();
    }

    public DataRow(FieldDefs fieldDefs) {
        super();
        this.fields = fieldDefs;
    }

    public DataRowState state() {
        return this.state;
    }

    @Deprecated
    public final DataRowState getState() {
        return this.state();
    }

    public DataRow setState(DataRowState value) {
        if (state != value) {
            if ((this.state == DataRowState.Insert) && (value == DataRowState.Update))
                throw new RuntimeException("change state error: insert => update");
            this.state = value;
            if (this.state == DataRowState.None)
                this.setHistory(null);
            else if (this.state == DataRowState.Update)
                this.setHistory(this.clone());
        }
        return this;
    }

    @Override
    public DataRow setValue(String field, Object value) {
        if (field == null || "".equals(field))
            throw new RuntimeException("field is null!");
        this.addField(field);

        SearchDataSet search = null;
        if (this.dataSet != null && this.dataSet.search() != null)
            search = this.dataSet.search();

        Object newValue = value;
        if (value instanceof Datetime) // 将Datetime转化为Date存储
            newValue = ((Datetime) value).asBaseDate();
        if (value != null && value.getClass().isEnum())
            newValue = ((Enum<?>) value).ordinal();

        if ((search == null) && (this.state != DataRowState.Update)) {
            putValue(field, newValue);
            return this;
        }

        Object curValue = items.get(field);
        if (compareValue(newValue, curValue))
            return this;

        // 只有值发生变更的时候 才做处理
        if (search != null) {
            search.remove(this);
            putValue(field, newValue);
            search.append(this);
        } else {
            putValue(field, newValue);
        }
        return this;
    }

    private void putValue(String field, Object value) {
        if (value == null || value instanceof Integer || value instanceof Double || value instanceof Boolean
                || value instanceof Long) {
            items.put(field, value);
        } else if (value instanceof String) {
            if ("{}".equals(value)) {
                items.put(field, null);
            } else {
                items.put(field, value);
            }
        } else if (value instanceof BigDecimal) {
            items.put(field, ((BigDecimal) value).doubleValue());
        } else if (value instanceof LinkedTreeMap) {
            items.put(field, null);
        } else {
            items.put(field, value);
        }
    }

    private boolean compareValue(Object value, Object compareValue) {
        // 都为空
        if (value == null && compareValue == null) {
            return true;
        }
        // 都不为空
        if (value != null && compareValue != null) {
            if ((value instanceof Integer) && (compareValue instanceof Double)) {
                Integer v1 = (Integer) value;
                Double v2 = (Double) compareValue;
                return v2 - v1 == 0;
            } else {
                return value.equals(compareValue);
            }
        } else {
            return false;
        }
    }

    @Override
    public Object getValue(String field) {
        if (field == null || "".equals(field)) {
            throw new RuntimeException("field is null!");
        }
        return this.items.get(field);
    }

    public Map<String, Object> delta() {
        Map<String, Object> delta = new HashMap<>();
        if (this.state() == DataRowState.Update) {
            for (String field : fields.names()) {
                Object oldValue = this.history.getValue(field);
                Object curValue = this.getValue(field);
                if (!this.compareValue(oldValue, curValue))
                    delta.put(field, oldValue);
            }
        }
        return delta;
    }

    @Deprecated
    public final Map<String, Object> getDelta() {
        return delta();
    }

    public Object getOldField(String field) {
        if (field == null || "".equals(field))
            throw new RuntimeException("field is null!");
        if (this.history != null)
            return this.history.getValue(field);
        else
            return null;
    }

    public int size() {
        return items.size();
    }

    public Map<String, Object> items() {
        return this.items;
    }

    @Deprecated
    public final Map<String, Object> getItems() {
        return items();
    }

    public FieldDefs fields() {
        return fields;
    }

    @Deprecated
    public final FieldDefs getFieldDefs() {
        return fields();
    }

    public void copyValues(DataRow source) {
        this.copyValues(source, source.fields());
    }

    public void copyValues(DataRow source, FieldDefs defs) {
        List<String> tmp = defs.names();
        String[] items = new String[defs.size()];
        for (int i = 0; i < defs.size(); i++) {
            items[i] = tmp.get(i);
        }
        copyValues(source, items);
        items = null;
    }

    public void copyValues(DataRow source, String... fields) {
        if (fields.length > 0) {
            for (String field : fields) {
                this.setValue(field, source.getValue(field));
            }
        } else {
            for (String field : source.fields().names()) {
                this.setValue(field, source.getValue(field));
            }
        }
    }

    @Override
    public String toString() {
        return json();
    }

    public String json() {
        Map<String, Object> items = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i).code();
            Object obj = this.getValue(field);
            if (obj instanceof Datetime) {
                items.put(field, obj.toString());
            } else if (obj instanceof Date) {
                items.put(field, (new Datetime((Date) obj)).toString());
            } else {
                items.put(field, obj);
            }
        }

        JsonSerializer<Double> gsonDouble = (src, typeOfSrc, context) -> {
            if (src == src.longValue())
                return new JsonPrimitive(src.longValue());
            return new JsonPrimitive(src);
        };

        Gson gson = new GsonBuilder().registerTypeAdapter(Double.class, gsonDouble).serializeNulls().create();
        return gson.toJson(items);
    }

    @Deprecated
    public final void setJSON(Object jsonObj) {
        if (!(jsonObj instanceof Map<?, ?>)) {
            throw new RuntimeException("not support type：" + jsonObj.getClass().getName());
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> head = (Map<String, Object>) jsonObj;
        for (String field : head.keySet()) {
            Object obj = head.get(field);
            if (obj instanceof Double) {
                double tmp = (double) obj;
                if (tmp >= Integer.MIN_VALUE && tmp <= Integer.MAX_VALUE) {
                    Integer val = (int) tmp;
                    if (tmp == val) {
                        obj = val;
                    }
                }
            }
            setValue(field, obj);
        }
    }

    public DataRow setJson(String jsonStr) {
        this.clear();
        Gson gson = new GsonBuilder().serializeNulls().create();
        items = gson.fromJson(jsonStr, new TypeToken<Map<String, Object>>() {
        }.getType());
        for (String key : items.keySet()) {
            this.addField(key);
            if ("{}".equals(items.get(key))) {
                items.put(key, null);
            }
        }
        return this;
    }

    public double getDouble(String field, int scale) {
        double value = this.getDouble(field);
        return Utils.roundTo(value, scale);
    }

    /**
     * 防止注入攻击
     *
     * @param field 字段名
     * @return 返回安全的字符串
     */
    @Deprecated
    public final String getSafeString(String field) {
        String value = getString(field);
        return value == null ? "" : value.replaceAll("'", "''");
    }

    @Deprecated
    public final TDate getDate(String field) {
        return new TDate(this.getDateTime(field).getTimestamp());
    }

    @Deprecated
    public final TDateTime getDateTime(String field) {
        return new TDateTime(getDatetime(field).getTimestamp());
    }

    public void clear() {
        items.clear();
        this.setState(DataRowState.None);
        if (this.dataSet == null)
            fields.clear();
    }

    @Override
    public boolean exists(String field) {
        return this.fields.exists(field);
    }

    /**
     * 
     * @param field
     * @return 判断是否有此栏位，以及此栏位是否有值
     */
    public boolean has(String field) {
        return fields.exists(field) && !"".equals(getString(field));
    }

    @Deprecated
    public final boolean hasValue(String field) {
        return has(field);
    }

    public DataSet dataSet() {
        return dataSet;
    }

    @Deprecated
    public final DataSet getDataSet() {
        return dataSet();
    }

    @Deprecated
    public final DataSet locate() {
        int recNo = dataSet.getRecords().indexOf(this) + 1;
        dataSet.setRecNo(recNo);
        return dataSet;
    }

    public boolean equalsValues(Map<String, Object> values) {
        for (String field : values.keySet()) {
            Object obj1 = getValue(field);
            String value = obj1 == null ? "null" : obj1.toString();
            Object obj2 = values.get(field);
            String compareValue = obj2 == null ? "null" : obj2.toString();
            if (!value.equals(compareValue)) {
                return false;
            }
        }
        return true;
    }

    public void remove(String field) {
        items.remove(field);
        if (history != null)
            history.remove(field);
        if (this.dataSet == null)
            fields.remove(field);
    }

    @Deprecated
    public final void delete(String field) {
        remove(field);
    }

    private void addField(String field) {
        if (field == null)
            throw new RuntimeException("field is null");
        if (!fields.exists(field)) {
            fields.add(field);
        }
    }

    public <T> T asEntity(Class<T> clazz) {
        T result = null;
        try {
            result = clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        saveToEntity(result);
        return result;
    }

    public void saveToEntity(Object entity) {
        try {
            Map<String, Field> items = DataRow.getEntityFields(entity.getClass());
            if (this.fields().size() > items.size()) {
                log.warn("fields.size > propertys.size");
            } else if (this.fields().size() < items.size()) {
                String fmt = "fields.size %d < propertys.size %d";
                throw new RuntimeException(String.format(fmt, this.fields().size(), items.size()));
            }

            // 查找并赋值
            for (FieldMeta meta : this.fields()) {
                Object value = this.getValue(meta.code());

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
                    } else if (value.getClass() == Long.class && field.getType() == Integer.class) {
                        Long tmp = (Long) value;
                        field.set(entity, tmp.intValue());
                    } else {
                        throw new RuntimeException(String.format("field %s error: %s as %s", field.getName(),
                                value.getClass().getName(), field.getType().getName()));
                    }
                }
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public DataRow loadFromEntity(Object entity) {
        try {
            Map<String, Field> fields = DataRow.getEntityFields(entity.getClass());
            for (String fieldCode : fields.keySet()) {
                Field field = fields.get(fieldCode);
                this.setValue(fieldCode, field.get(entity));
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return this;
    }

    public static Map<String, Field> getEntityFields(Class<?> entityClass) {
        // 找出所有可用的的数据字段
        Map<String, Field> items = new LinkedHashMap<>();
        for (Field field : entityClass.getDeclaredFields()) {
            Column column = field.getAnnotation(Column.class);
            if (column != null) {
                String name = !"".equals(column.name()) ? column.name() : field.getName();
                if (field.getModifiers() == DataRow.PRIVATE || field.getModifiers() == DataRow.PROTECTED) {
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

    @Deprecated
    public final <T> T asObject(Class<T> clazz) {
        return asEntity(clazz);
    }

    public String getText(String field) {
        FieldMeta meta = this.fields().get(field);
        return meta.getText(this);
    }

    public DataRow setText(String field, String value) {
        FieldMeta meta = this.fields().get(field);
        this.setValue(field, meta.setText(value));
        return this;
    }

    public HashSet<FieldMeta> getFields() {
        return this.fields().getItems();
    }

    public final DataRow history() {
        return history;
    }

    public DataRow setHistory(DataRow history) {
        this.history = history;
        if (this.history != null)
            this.history.setState(DataRowState.History);
        return this;
    }

    @Override
    public DataRow clone() {
        DataRow row = new DataRow(this.fields);
        for (String key : this.fields().names())
            row.setValue(key, this.getValue(key));
        row.dataSet = this.dataSet;
        row.createFields = this.createFields;
        return row;
    }

}
