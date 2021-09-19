package cn.cerc.core;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;

public class DataRow implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(DataRow.class);
    private static final long serialVersionUID = 4454304132898734723L;
    private RecordState state = RecordState.dsNone;
    private Map<String, Object> items = new LinkedHashMap<>();
    private Map<String, Object> delta = new HashMap<>();
    private DataSet dataSet;
    private FieldDefs defs;

    public DataRow() {
        super();
        this.defs = new FieldDefs();
    }

    public DataRow(DataSet dataSet) {
        super();
        this.dataSet = dataSet;
        this.defs = dataSet.getFieldDefs();
    }

    public RecordState getState() {
        return state;
    }

    public DataRow setState(RecordState recordState) {
        if (recordState == RecordState.dsEdit) {
            if (this.state == RecordState.dsInsert) {
                // throw new RuntimeException("当前记录为插入状态 不允许被修改");
                return this;
            }
        }
        if (recordState.equals(RecordState.dsNone)) {
            delta.clear();
        }
        this.state = recordState;
        return this;
    }

    public DataRow setField(String field, Object value) {
        if (field == null || "".equals(field)) {
            throw new RuntimeException("field is null!");
        }
        this.addFieldDef(field);

        SearchDataSet search = null;
        if (this.dataSet != null && this.dataSet.getSearch() != null)
            search = this.dataSet.getSearch();

        Object data = value;
        if (value instanceof Datetime) // 将Datetime转化为Date存储
            data = ((Datetime) value).asBaseDate();

        if ((search == null) && (this.state != RecordState.dsEdit)) {
            setValue(items, field, data);
            return this;
        }

        Object oldValue = items.get(field);
        if (compareValue(data, oldValue)) {
            return this;
        }

        // 只有值发生变更的时候 才做处理
        if (search != null)
            search.remove(this);

        if (this.state == RecordState.dsEdit) {
            if (!delta.containsKey(field)) {
                setValue(delta, field, oldValue);
            }
        }
        setValue(items, field, data);

        if (search != null)
            search.append(this);
        return this;
    }

    private void setValue(Map<String, Object> map, String field, Object value) {
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

    public Object getField(String field) {
        if (field == null || "".equals(field)) {
            throw new RuntimeException("field is null!");
        }
        return items.get(field);
    }

    public Map<String, Object> getDelta() {
        return delta;
    }

    public Object getOldField(String field) {
        if (field == null || "".equals(field)) {
            throw new RuntimeException("field is null!");
        }
        return delta.get(field);
    }

    public int size() {
        return items.size();
    }

    public Map<String, Object> getItems() {
        return this.items;
    }

    public FieldDefs getFieldDefs() {
        return defs;
    }

    public void copyValues(DataRow source) {
        this.copyValues(source, source.getFieldDefs());
    }

    public void copyValues(DataRow source, FieldDefs defs) {
        List<String> tmp = defs.getFields();
        String[] items = new String[defs.size()];
        for (int i = 0; i < defs.size(); i++) {
            items[i] = tmp.get(i);
        }
        copyValues(source, items);
    }

    public void copyValues(DataRow source, String... fields) {
        if (fields.length > 0) {
            for (String field : fields) {
                this.setField(field, source.getField(field));
            }
        } else {
            for (String field : source.getFieldDefs().getFields()) {
                this.setField(field, source.getField(field));
            }
        }
    }

    @Override
    public String toString() {
        Map<String, Object> items = new LinkedHashMap<>();
        for (int i = 0; i < defs.size(); i++) {
            String field = defs.get(i).getCode();
            Object obj = this.getField(field);
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
            setField(field, obj);
        }
    }

    public void setJSON(String jsonStr) {
        this.clear();
        Gson gson = new GsonBuilder().serializeNulls().create();
        items = gson.fromJson(jsonStr, new TypeToken<Map<String, Object>>() {
        }.getType());
        for (String key : items.keySet()) {
            this.addFieldDef(key);
            if ("{}".equals(items.get(key))) {
                items.put(key, null);
            }
        }
    }

    public boolean getBoolean(String field) {
        this.addFieldDef(field);
        Object obj = this.getField(field);
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        } else if (obj instanceof String) {
            String str = (String) obj;
            return !"".equals(str) && !"0".equals(str) && !"false".equals(str);
        } else if (obj instanceof Integer) {
            int value = (Integer) obj;
            return value > 0;
        } else {
            return false;
        }
    }

    public int getInt(String field) {
        this.addFieldDef(field);
        Object obj = this.getField(field);
        if (obj instanceof Integer) {
            return (Integer) obj;
        } else if (obj instanceof BigInteger) {
            StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
            for (StackTraceElement item : stacktrace) {
                log.warn("{}.{}:{}", item.getClassName(), item.getMethodName(), item.getLineNumber());
            }
            log.warn("type error: getInt() can not use BigInteger");
            return ((BigInteger) obj).intValue();
        } else if (obj instanceof Double) {
            return ((Double) obj).intValue();
        } else if (obj instanceof String) {
            String str = (String) obj;
            if ("".equals(str)) {
                return 0;
            }
            double val = Double.parseDouble(str);
            return (int) val;
        } else if (obj instanceof Long) {
            return ((Long) obj).intValue();
        } else if ((obj instanceof Boolean)) {
            return (Boolean) obj ? 1 : 0;
        } else if ((obj instanceof Short)) {
            return ((Short) obj).intValue();
        } else {
            return 0;
        }
    }

    public long getLong(String field) {
        this.addFieldDef(field);
        Object obj = this.getField(field);
        if (obj instanceof Integer) {
            return (Integer) obj;
        } else if (obj instanceof BigInteger) {
            StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
            for (StackTraceElement item : stacktrace) {
                log.warn("{}.{}:{}", item.getClassName(), item.getMethodName(), item.getLineNumber());
            }
            log.warn("type error: getInt() can not use BigInteger");
            return ((BigInteger) obj).intValue();
        } else if (obj instanceof Double) {
            return ((Double) obj).intValue();
        } else if (obj instanceof String) {
            String str = (String) obj;
            if ("".equals(str)) {
                return 0;
            }
            Long val = Long.parseLong(str);
            return val;
        } else if (obj instanceof Long) {
            return ((Long) obj);
        } else if ((obj instanceof Boolean)) {
            return (Boolean) obj ? 1 : 0;
        } else if ((obj instanceof Short)) {
            return ((Short) obj).intValue();
        } else {
            return 0;
        }
    }

    public BigInteger getBigInteger(String field) {
        this.addFieldDef(field);
        Object obj = this.getField(field);
        if (obj instanceof BigInteger) {
            return (BigInteger) obj;
        } else if (obj instanceof String) {
            String str = (String) obj;
            if ("".equals(str)) {
                return BigInteger.valueOf(0);
            }
            return new BigInteger(str);
        } else if (obj instanceof Integer) {
            return BigInteger.valueOf((Integer) obj);
        } else if ((obj instanceof Short)) {
            return BigInteger.valueOf((Short) obj);
        } else if (obj instanceof Double) {
            return BigInteger.valueOf(((Double) obj).longValue());
        } else if (obj instanceof Long) {
            return BigInteger.valueOf((Long) obj);
        } else {
            return BigInteger.valueOf(0);
        }
    }

    public BigDecimal getBigDecimal(String field) {
        this.addFieldDef(field);
        Object obj = this.getField(field);
        if (obj instanceof BigInteger) {
            return (BigDecimal) obj;
        } else if (obj instanceof String) {
            String str = (String) obj;
            if ("".equals(str)) {
                return BigDecimal.valueOf(0);
            }
            return new BigDecimal(str);
        } else if (obj instanceof Integer) {
            return BigDecimal.valueOf((Integer) obj);
        } else if (obj instanceof Short) {
            return BigDecimal.valueOf((Short) obj);
        } else if (obj instanceof Double) {
            return BigDecimal.valueOf(((Double) obj).longValue());
        } else if (obj instanceof Long) {
            return BigDecimal.valueOf((Long) obj);
        } else {
            return BigDecimal.valueOf(0);
        }
    }

    public double getDouble(String field) {
        this.addFieldDef(field);
        Object obj = this.getField(field);
        if (obj instanceof String) {
            String str = (String) obj;
            if ("".equals(str)) {
                return 0;
            }
            return Double.parseDouble((String) obj);
        }
        if (obj instanceof Integer) {
            return ((Integer) obj) * 1.0;
        } else if ((obj instanceof Short)) {
            return ((Short) obj) * 1.0;
        } else if (obj == null) {
            return 0.0;
        } else if (obj instanceof BigInteger) {
            return ((BigInteger) obj).doubleValue();
        } else if (obj instanceof Long) {
            Long tmp = (Long) obj;
            return tmp * 1.0;
        } else if ((obj instanceof Boolean)) {
            return (Boolean) obj ? 1 : 0;
        } else {
            double d = (Double) obj;
            if (d == 0) {
                d = 0;
            }
            return d;
        }
    }

    public double getDouble(String field, int digit) {
        double result = this.getDouble(field);
        String str = "0.00000000";
        str = str.substring(0, str.indexOf(".") + (-digit) + 1);
        DecimalFormat df = new DecimalFormat(str);
        return Double.parseDouble(df.format(result));
    }

    public String getString(String field) {
        this.addFieldDef(field);
        String result = "";
        Object obj = this.getField(field);
        if (obj != null) {
            if (obj instanceof String) {
                result = (String) obj;
            } else if (obj instanceof Date) {
                Datetime tmp = new Datetime(((Date) obj).getTime());
                result = tmp.isEmpty() ? "" : tmp.toString();
            } else if (obj instanceof Double) {
                Double temp = (Double) obj;
                long value = temp.longValue();
                if (temp == value) {
                    result = String.valueOf(value);
                } else {
                    result = temp.toString();
                }
            } else {
                result = obj.toString();
            }
        }
        return result;
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

    public Datetime getDatetime(String field) {
        this.addFieldDef(field);
        Object obj = this.getField(field);
        if (obj == null) {
            return Datetime.zero();
        } else if (obj instanceof String) {
            return new Datetime((String) obj);
        } else if (obj instanceof Date) {
            return new Datetime((Date) obj);
        } else {
            throw new RuntimeException(String.format("%s Field not is %s.", field, obj.getClass().getName()));
        }
    }

    public FastDate getFastDate(String field) {
        return this.getDatetime(field).toFastDate();
    }

    public FastTime getFastTime(String field) {
        return this.getDatetime(field).toFastTime();
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
            defs.clear();
    }

    public boolean exists(String field) {
        return this.defs.exists(field);
    }

    public boolean hasValue(String field) {
        return defs.exists(field) && !"".equals(getString(field));
    }

    public DataSet getDataSet() {
        return dataSet;
    }

    @Deprecated
    public DataSet locate() {
        int recNo = dataSet.getRecords().indexOf(this) + 1;
        dataSet.setRecNo(recNo);
        return dataSet;
    }

    public boolean isModify() {
        switch (this.state) {
        case dsInsert:
            return true;
        case dsEdit: {
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
            Object obj1 = getField(field);
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
        delta.remove(field);
        items.remove(field);
        if (this.dataSet == null) {
            defs.delete(field);
        }
    }

    private void addFieldDef(String field) {
        if (field == null)
            throw new RuntimeException("field is null");
        if (!defs.exists(field)) {
            defs.add(field);
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
        FieldMeta meta = this.getFieldDefs().get(field);
        return meta.getText(this);
    }

    public DataRow setText(String field, String value) {
        FieldMeta meta = this.getFieldDefs().get(field);
        this.setField(field, meta.setText(value));
        return this;
    }

}
