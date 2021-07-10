package cn.cerc.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchDataSet {
    private DataSet dataSet;
    private Map<String, Record> items;
    private List<String> keys = new ArrayList<>();
    private String fields;

    public SearchDataSet() {

    }

    public SearchDataSet(DataSet dataSet) {
        this.dataSet = dataSet;
    }

    public SearchDataSet add(DataSet dataSet) {
        for (int i = dataSet.size(); i > 0; i--) {
            add(dataSet.getRecords().get(i - 1));
        }
        return this;
    }

    public void add(Record record) {
        if (items == null) {
            items = new HashMap<>();
        }
        String key = null;
        for (String field : keys) {
            Object val = record.getField(field);
            if (val == null) {
                val = "null";
            }
            key = key == null ? val.toString() : key + ";" + val.toString();
        }
        items.put(key, record);
    }

    public Record get(Object value) {
        if (items == null) {
            items = new HashMap<>();
            add(dataSet);
        }
        if (value == null) {
            return items.get("null");
        } else {
            return items.get(value.toString());
        }
    }

    public Record get(Object[] values) {
        if (values == null || values.length == 0) {
            throw new RuntimeException("keys can't be null or keys's length = 0 ");
        }
        if (keys.size() != values.length) {
            throw new RuntimeException("参数名称 与 值列表长度不匹配");
        }

        String value = null;
        for (Object obj : values) {
            if (obj == null) {
                obj = "null";
            }
            value = value == null ? obj.toString() : value + ";" + obj.toString();
        }

        return get(value);
    }

    public void clear() {
        this.fields = null;
        keys.clear();
        items = null;
    }

    public String getFields() {
        return fields;
    }

    public void setFields(String fields) {
        if (fields == null || "".equals(fields))
            throw new RuntimeException("keyFields can't be null");
        if (!fields.equals(this.fields)) {
            this.clear();
            for (String key : fields.split(";")) {
                if (dataSet.size() > 0 && dataSet.getFieldDefs().size() > 0 && !dataSet.exists(key)) {
                    throw new RuntimeException(String.format("field %s not find !", key));
                }
                keys.add(key);
            }
            this.fields = fields;
        }
    }

    public boolean existsKey(String field) {
        return keys.contains(field);
    }
}
