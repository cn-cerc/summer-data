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
    private DataRowState _state = DataRowState.None;
    private Map<String, Object> _items = new LinkedHashMap<>();
    private Map<String, Object> _delta = new HashMap<>();
    private DataSet _dataSet;
    private FieldDefs _fields;
    private boolean _createFieldDefs;
    private DataRow _history;

    public DataRow() {
        super();
        this._fields = new FieldDefs();
        this._createFieldDefs = true;
    }

    public DataRow(DataSet dataSet) {
        super();
        this._dataSet = dataSet;
        this._fields = dataSet.fields();
    }

    public DataRow(FieldDefs fieldDefs) {
        super();
        this._fields = fieldDefs;
    }

    public DataRowState state() {
        return this._state;
    }

    @Deprecated
    public DataRowState getState() {
        return this.state();
    }

    public DataRow setState(DataRowState value) {
        if (_state == value)
            return this;

        if (value.equals(DataRowState.None))
            _delta.clear();

        if ((_state == DataRowState.Insert) && (value == DataRowState.Update))
            return this;
        if ((_state == DataRowState.None) && (value == DataRowState.Update)) {
            this._history = this.clone();
            this._history._state = DataRowState.History;
            _state = value;
            return this;
        }
        if ((_state == DataRowState.None) && (value == DataRowState.Update)) {
            this._history = null;
            _state = value;
            return this;
        }

        if ((_state == DataRowState.None) || (value == DataRowState.None))
            this._state = value;
        else if ((_state == DataRowState.Insert) || (value == DataRowState.History))
            this._state = value;
        else
            throw new RuntimeException("setState change error");
        return this;
    }

    @Override
    public DataRow setValue(String field, Object value) {
        if (field == null || "".equals(field))
            throw new RuntimeException("field is null!");

        this.addFieldDef(field);

        SearchDataSet search = null;
        if (this._dataSet != null && this._dataSet.search() != null)
            search = this._dataSet.search();

        Object data = value;
        if (value instanceof Datetime) // 将Datetime转化为Date存储
            data = ((Datetime) value).asBaseDate();

        if ((search == null) && (this._state != DataRowState.Update)) {
            setMapValue(_items, field, data);
            return this;
        }

        Object oldValue = _items.get(field);
        if (compareValue(data, oldValue)) {
            return this;
        }

        // 只有值发生变更的时候 才做处理
        if (search != null)
            search.remove(this);

        if (this._state == DataRowState.Update) {
            if (!_delta.containsKey(field)) {
                setMapValue(_delta, field, oldValue);
            }
        }
        setMapValue(_items, field, data);

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
        return this._items.get(field);
    }

    public Map<String, Object> getDelta() {
        return this._delta;
    }

    public Object getOldField(String field) {
        if (field == null || "".equals(field)) {
            throw new RuntimeException("field is null!");
        }
        return this._delta.get(field);
    }

    public int size() {
        return _items.size();
    }

    public Map<String, Object> items() {
        return this._items;
    }

    @Deprecated
    public Map<String, Object> getItems() {
        return items();
    }

    public FieldDefs fields() {
        return _fields;
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
        Map<String, Object> items = new LinkedHashMap<>();
        for (int i = 0; i < _fields.size(); i++) {
            String field = _fields.getItems(i).getCode();
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

    public void setJSON(String jsonStr) {
        this.clear();
        Gson gson = new GsonBuilder().serializeNulls().create();
        _items = gson.fromJson(jsonStr, new TypeToken<Map<String, Object>>() {
        }.getType());
        for (String key : _items.keySet()) {
            this.addFieldDef(key);
            if ("{}".equals(_items.get(key))) {
                _items.put(key, null);
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
        _items.clear();
        _delta.clear();
        if (this._dataSet == null)
            _fields.clear();
    }

    @Override
    public boolean exists(String field) {
        return this._fields.exists(field);
    }

    public boolean hasValue(String field) {
        return _fields.exists(field) && !"".equals(getString(field));
    }

    public DataSet dataSet() {
        return _dataSet;
    }

    @Deprecated
    public DataSet getDataSet() {
        return dataSet();
    }

    @Deprecated
    public DataSet locate() {
        int recNo = _dataSet.getRecords().indexOf(this) + 1;
        _dataSet.setRecNo(recNo);
        return _dataSet;
    }

    public boolean isModify() {
        switch (this._state) {
        case Insert:
            return true;
        case Update: {
            if (_delta.size() == 0) {
                return false;
            }
            List<String> delList = new ArrayList<>();
            for (String field : _delta.keySet()) {
                Object value = _items.get(field);
                Object oldValue = _delta.get(field);
                if (compareValue(value, oldValue)) {
                    delList.add(field);
                }
            }
            for (String field : delList) {
                _delta.remove(field);
            }
            return _delta.size() > 0;
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

    public void delete(String field) {
        _delta.remove(field);
        _items.remove(field);
        if (this._dataSet == null)
            _fields.remove(field);
    }

    private void addFieldDef(String field) {
        if (field == null)
            throw new RuntimeException("field is null");
        if (!_fields.exists(field)) {
            _fields.add(field);
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
        return _history;
    }

    public final DataRow setHistory(DataRow history) {
        this._history = history;
        return this;
    }

    @Override
    public DataRow clone() {
        DataRow row = new DataRow(this._fields);
        for (String key : this.fields().names())
            row.setValue(key, this.getValue(key));
        row._dataSet = this._dataSet;
        row._createFieldDefs = this._createFieldDefs;
        return row;
    }

}
