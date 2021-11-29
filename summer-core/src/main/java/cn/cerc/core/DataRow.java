package cn.cerc.core;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;

public class DataRow implements Serializable, IRecord {
    private static final long serialVersionUID = 4454304132898734723L;
    private DataRowState state = DataRowState.None;
    private Map<String, Object> items = new LinkedHashMap<>();
    private Map<String, Object> delta = new HashMap<>();
    private DataSet dataSet;
    private FieldDefs fields;
    private boolean createFieldDefs;
    private DataRow history;

    public DataRow() {
        super();
        this.fields = new FieldDefs();
        this.createFieldDefs = true;
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
    public DataRowState getState() {
        return this.state();
    }

    public DataRow setState(DataRowState value) {
        if (state == value)
            return this;

        if (value.equals(DataRowState.None))
            delta.clear();

        if ((state == DataRowState.Insert) && (value == DataRowState.Update))
            return this;
        if ((state == DataRowState.None) && (value == DataRowState.Update)) {
            this.history = this.clone();
            this.history.state = DataRowState.History;
            state = value;
            return this;
        }
        if ((state == DataRowState.None) && (value == DataRowState.Update)) {
            this.history = null;
            state = value;
            return this;
        }

        if ((state == DataRowState.None) || (value == DataRowState.None))
            this.state = value;
        else if ((state == DataRowState.Insert) || (value == DataRowState.History))
            this.state = value;
        else
            throw new RuntimeException("setState change error");
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

        Object data = value;
        if (value instanceof Datetime) // 将Datetime转化为Date存储
            data = ((Datetime) value).asBaseDate();

        if ((search == null) && (this.state != DataRowState.Update)) {
            setMapValue(items, field, data);
            return this;
        }

        Object oldValue = items.get(field);
        if (compareValue(data, oldValue)) {
            return this;
        }

        // 只有值发生变更的时候 才做处理
        if (search != null)
            search.remove(this);

        if (this.state == DataRowState.Update) {
            if (!delta.containsKey(field)) {
                setMapValue(delta, field, oldValue);
            }
        }
        setMapValue(items, field, data);

        if (search != null)
            search.append(this);

        return this;
    }

    private void setMapValue(Map<String, Object> map, String field, Object value) {
        if (value == null || value instanceof Integer || value instanceof Double || value instanceof Boolean
                || value instanceof Long) {
            map.put(field, value);
        } else if (value instanceof String) {
            if ("{}".equals(value)) {
                map.put(field, null);
            } else {
                map.put(field, value);
            }
        } else if (value instanceof BigDecimal) {
            map.put(field, ((BigDecimal) value).doubleValue());
        } else if (value instanceof LinkedTreeMap) {
            map.put(field, null);
        } else {
            map.put(field, value);
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

    public Map<String, Object> getDelta() {
        return this.delta;
    }

    public Object getOldField(String field) {
        if (field == null || "".equals(field)) {
            throw new RuntimeException("field is null!");
        }
        return this.delta.get(field);
    }

    public int size() {
        return items.size();
    }

    public Map<String, Object> items() {
        return this.items;
    }

    @Deprecated
    public Map<String, Object> getItems() {
        return items();
    }

    public FieldDefs fields() {
        return fields;
    }

    @Deprecated
    public FieldDefs getFieldDefs() {
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
            String field = fields.getItems(i).getCode();
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

    public void setJSON(Object jsonObj) {
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

    public void setJson(String jsonStr) {
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
    public String getSafeString(String field) {
        String value = getString(field);
        return value == null ? "" : value.replaceAll("'", "''");
    }

    @Deprecated
    public TDate getDate(String field) {
        return new TDate(this.getDateTime(field).getTimestamp());
    }

    @Deprecated
    public TDateTime getDateTime(String field) {
        return new TDateTime(getDatetime(field).getTimestamp());
    }

    public void clear() {
        items.clear();
        delta.clear();
        if (this.dataSet == null)
            fields.clear();
    }

    @Override
    public boolean exists(String field) {
        return this.fields.exists(field);
    }

    public boolean hasValue(String field) {
        return fields.exists(field) && !"".equals(getString(field));
    }

    public DataSet dataSet() {
        return dataSet;
    }

    @Deprecated
    public DataSet getDataSet() {
        return dataSet();
    }

    @Deprecated
    public DataSet locate() {
        int recNo = dataSet.getRecords().indexOf(this) + 1;
        dataSet.setRecNo(recNo);
        return dataSet;
    }

    public boolean isModify() {
        switch (this.state) {
        case Insert:
            return true;
        case Update: {
            if (delta.size() == 0) {
                return false;
            }
            List<String> delList = new ArrayList<>();
            for (String field : delta.keySet()) {
                Object value = items.get(field);
                Object oldValue = delta.get(field);
                if (compareValue(value, oldValue)) {
                    delList.add(field);
                }
            }
            for (String field : delList) {
                delta.remove(field);
            }
            return delta.size() > 0;
        }
        default:
            return false;
        }
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
        delta.remove(field);
        items.remove(field);
        if (this.dataSet == null)
            fields.remove(field);
    }

    @Deprecated
    public void delete(String field) {
        remove(field);
    }

    private void addField(String field) {
        if (field == null)
            throw new RuntimeException("field is null");
        if (!fields.exists(field)) {
            fields.add(field);
        }
    }

    public <T> T asObject(Class<T> clazz) {
        T result = null;
        try {
            result = clazz.getDeclaredConstructor().newInstance();
            RecordUtils.copyToObject(this, result);
            return result;
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e1) {
            e1.printStackTrace();
            return null;
        }
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

    public final DataRow setHistory(DataRow history) {
        this.history = history;
        return this;
    }

    @Override
    public DataRow clone() {
        DataRow row = new DataRow(this.fields);
        for (String key : this.fields().names())
            row.setValue(key, this.getValue(key));
        row.dataSet = this.dataSet;
        row.createFieldDefs = this.createFieldDefs;
        return row;
    }

}
