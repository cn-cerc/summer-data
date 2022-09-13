package cn.cerc.db.core;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.persistence.Column;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;

import cn.cerc.db.Alias;

public class DataRow implements Serializable, IRecord {
    private static final Logger log = LoggerFactory.getLogger(DataRow.class);
    private static final long serialVersionUID = 4454304132898734723L;
    private DataRowState state = DataRowState.None;
    private Map<String, Object> items = new LinkedHashMap<>();
    private DataSet dataSet;
    private FieldDefs fields;
    private boolean createFields;
    private DataRow history;
    private boolean readonly;

    public DataRow() {
        super();
        this.fields = new FieldDefs();
        this.createFields = true;
    }

    public DataRow(FieldDefs fieldDefs) {
        super();
        this.fields = fieldDefs;
    }

    public DataRow(DataSet dataSet) {
        super();
        this.setDataSet(dataSet);
    }

    public DataRowState state() {
        return this.state;
    }

    /**
     * 替换掉 Map.of 的传值方式，根据字段参数直接生成 DataRow
     */
    public static DataRow of(Object... args) {
        if (args.length % 2 != 0)
            throw new RuntimeException("dataRow 传入参数数量必须为偶数");

        DataRow dataRow = new DataRow();
        for (int i = 0; i + 2 <= args.length; i = i + 2) {
            String field = (String) args[i];
            if (Utils.isEmpty(field))
                throw new RuntimeException("field 字段不允许为空");

            Object value = args[i + 1];
            if (value == null)
                value = "";
            dataRow.setValue(field, value);
        }
        return dataRow;
    }

    @Deprecated
    public final DataRowState getState() {
        return this.state();
    }

    public DataRow setState(DataRowState value) {
        if (this.readonly())
            throw new UnsupportedOperationException("DataRow is readonly");
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
        if (this.readonly())
            throw new UnsupportedOperationException("DataRow is readonly");
        if (field == null || "".equals(field))
            throw new RuntimeException("field is null!");
        this.addField(field);

        SearchDataSet search = null;
        if (this.dataSet != null && this.dataSet.search() != null)
            search = this.dataSet.search();

        Object newValue = value;
        if (value instanceof Datetime) // 将Datetime转化为Date存储
            newValue = ((Datetime) value).asBaseDate();
        else if (value instanceof Optional<?>)
            newValue = ((Optional<?>) value).orElse(null);
        else if (value != null && value.getClass().isEnum())
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

    @Deprecated
    public Object getOldField(String field) {
        return this.getOldValue(field);
    }

    public Object getOldValue(String field) {
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

    @Deprecated
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
        if (this.readonly())
            throw new UnsupportedOperationException("DataRow is readonly");
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
        if (!(jsonObj instanceof Map<?, ?>))
            throw new RuntimeException("not support type：" + jsonObj.getClass().getName());
        if (this.readonly())
            throw new UnsupportedOperationException("DataRow is readonly");

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
        if (this.readonly())
            throw new UnsupportedOperationException("DataRow is readonly");
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
     * @param field 字段代码
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
        if (this.readonly())
            throw new UnsupportedOperationException("DataRow is readonly");
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
        if (this.readonly())
            throw new UnsupportedOperationException("DataRow is readonly");
        if (!fields.exists(field))
            fields.add(field);
    }

    public <T extends EntityImpl> T asEntity(Class<T> clazz) {
        EntityHelper<T> helper = EntityHelper.create(clazz);
        T entity = helper.newEntity();
        saveToEntity(entity);
        return entity;
    }

    public void saveToEntity(EntityImpl entity) {
        EntityHelper<? extends EntityImpl> helper = EntityHelper.create(entity.getClass());
        Map<String, Field> items = helper.fields();
        if (this.fields().size() > items.size()) {
            log.warn("database fields.size > entity properties.size");
        } else if (this.fields().size() < items.size()) {
            throw new RuntimeException(String.format("database fields.size %d < %s properties.size %d ",
                    this.fields().size(), entity.getClass().getName(), items.size()));
        }

        // 查找并赋值
        Variant variant = new Variant();
        for (FieldMeta meta : this.fields()) {
            Object value = this.getValue(meta.code());

            // 查找指定的对象属性
            Field field = items.get(meta.code());
            if (field == null) {
                log.warn("not find property: " + meta.code());
                continue;
            }

            // 给属性赋值
            try {
                if (value == null) {
                    Column column = field.getAnnotation(Column.class);
                    if (column == null || column.nullable()) {
                        field.set(entity, null);
                    } else
                        variant.setValue(null).writeToEntity(entity, field);
                } else if (field.getType().equals(value.getClass()))
                    field.set(entity, value);
                else
                    variant.setValue(value).writeToEntity(entity, field);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                e.printStackTrace();
                throw new RuntimeException(String.format("field %s error: %s as %s", field.getName(),
                        value.getClass().getName(), field.getType().getName()));
            }
        }
    }

    public <T extends EntityImpl> DataRow loadFromEntity(T entity) {
        try {
            Map<String, Field> fields = EntityHelper.create(entity.getClass()).fields();
            for (String fieldCode : fields.keySet())
                this.setValue(fieldCode, fields.get(fieldCode).get(entity));
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return this;
    }

    @Deprecated
    public final <T extends EntityImpl> T asObject(Class<T> clazz) {
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
        if (this.readonly())
            throw new UnsupportedOperationException("DataRow is readonly");
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

    public DataRow moveTo(DataSet target) {
        if (this.readonly())
            throw new UnsupportedOperationException("DataRow is readonly");
        if (dataSet == target)
            throw new IllegalArgumentException();
        if (dataSet != null) {
            dataSet.records().remove(this);
            dataSet.last();
        }
        this.setDataSet(target);
        target.records().add(this);
        target.last();
        return this;
    }

    public DataRow setDataSet(DataSet dataSet) {
        if (this.readonly())
            throw new UnsupportedOperationException("DataRow is readonly");
        Objects.requireNonNull(dataSet);
        if (this.dataSet != dataSet) {
            this.dataSet = dataSet;
            this.fields = dataSet.fields();
            this.createFields = false;
        }
        return this;
    }

    public boolean readonly() {
        return dataSet != null ? dataSet.readonly() : readonly;
    }

    public DataRow setReadonly(boolean readonly) {
        if (this.dataSet != null)
            throw new UnsupportedOperationException("DataRow is belong DataSet, can not be modify");
        this.readonly = readonly;
        return this;
    }

    public static class RecordProxy implements InvocationHandler {
        private final DataRow dataRow;

        public RecordProxy(DataRow dataRow) {
            this.dataRow = dataRow;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object result;
            String field = method.getName();
            Alias alias = method.getAnnotation(Alias.class);
            if (alias != null && alias.value().length() > 0)
                field = alias.value();
            if (dataRow.fields().get(field) == null)
                throw new RuntimeException("not find field: " + field);
            if (method.getReturnType() == Variant.class)
                result = dataRow.bind(field);
            else if (method.getReturnType() == String.class)
                result = dataRow.getString(field);
            else if (method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class)
                result = dataRow.getBoolean(field);
            else if (method.getReturnType() == int.class || method.getReturnType() == Integer.class)
                result = dataRow.getInt(field);
            else if (method.getReturnType() == double.class || method.getReturnType() == Double.class)
                result = dataRow.getDouble(field);
            else if (method.getReturnType() == long.class || method.getReturnType() == Long.class)
                result = dataRow.getLong(field);
            else if (method.getReturnType() == Datetime.class)
                result = dataRow.getDatetime(field);
            else
                result = dataRow.getValue(field);
            return result;
        }

    }

    @SuppressWarnings("unchecked")
    public <T> T asRecord(Class<T> clazz) {
        if (clazz.isInterface()) {
            return (T) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] { clazz },
                    new RecordProxy(this));
//        } else if (clazz.isRecord()) {
//            Constructor<?> constructor = clazz.getConstructors()[0];
//            Object[] initArgs = new Object[constructor.getParameterCount()];
//            int i = 0;
//            for (Parameter item : constructor.getParameters()) {
//                String field = item.getName();
//                Alias alias = item.getAnnotation(Alias.class);
//                if (alias != null && alias.value().length() > 0)
//                    field = alias.value();
//                if (item.getType() == Variant.class)
//                    initArgs[i++] = new Variant(this.getValue(field)).setKey(field);
//                else if (item.getType() == String.class)
//                    initArgs[i++] = this.getString(field);
//                else if (item.getType() == boolean.class || item.getType() == Boolean.class)
//                    initArgs[i++] = this.getBoolean(field);
//                else if (item.getType() == int.class || item.getType() == Integer.class)
//                    initArgs[i++] = this.getInt(field);
//                else if (item.getType() == double.class || item.getType() == Double.class)
//                    initArgs[i++] = this.getDouble(field);
//                else if (item.getType() == long.class || item.getType() == Long.class)
//                    initArgs[i++] = this.getLong(field);
//                else if (item.getType() == Datetime.class)
//                    initArgs[i++] = this.getDatetime(field);
//                else
//                    initArgs[i++] = this.getValue(field);
//            }
//            try {
//                return (T) constructor.newInstance(initArgs);
//            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
//                    | InvocationTargetException e) {
//                e.printStackTrace();
//                throw new RuntimeException(e.getMessage());
//            }
        } else
            throw new RuntimeException("only support record and interface");
    }

    public Variant bind(String field) {
        return new Variant(this, field);
    }
}
