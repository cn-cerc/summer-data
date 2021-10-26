package cn.cerc.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SearchDataSet {
    private final DataSet dataSet;
    private final HashMap<String, DataRow> items = new HashMap<>();
    private final List<String> keys = new ArrayList<>();
    private String fields;

    public SearchDataSet(DataSet dataSet) {
        this.dataSet = dataSet;
    }

    public final DataRow get(String fields, Object[] values) {
        if (fields == null || "".equals(fields))
            throw new RuntimeException("fields can't be null");
        if (values.length == 0)
            throw new RuntimeException("keys can't values length = 0 ");

        if (!fields.equals(this.fields)) {
            this.clear();
            this.fields = fields;
            for (String key : fields.split(";")) {
                if (dataSet.size() > 0 && dataSet.getFieldDefs().size() > 0 && !dataSet.exists(key)) {
                    throw new RuntimeException(String.format("field %s not find !", key));
                }
                keys.add(key);
            }
            // 重置索引
            if (keys.size() > 0) {
                for (int i = dataSet.size(); i > 0; i--)
                    append(dataSet.getRecords().get(i - 1));
            }
        }
        if (keys.size() != values.length)
            throw new RuntimeException("[参数名称]与[值]个数不匹配");

        return items.get(buildObjectKey(values));
    }

    public final void remove(DataRow record) {
        items.remove(buildRecordKey(record));
    }

    public final void append(DataRow record) {
        items.put(buildRecordKey(record), record);
    }

    public final void clear() {
        fields = null;
        keys.clear();
        items.clear();
    }

    private final String buildRecordKey(DataRow record) {
        String result = null;
        for (String field : keys) {
            Object val = record.getValue(field);
            if (val == null)
                val = "null";
            result = result == null ? val.toString() : result + ";" + val.toString();
        }
        return result;
    }

    private final String buildObjectKey(Object[] values) {
        String result = null;
        for (Object obj : values) {
            if (obj == null)
                obj = "null";
            result = result == null ? obj.toString() : result + ";" + obj.toString();
        }
        return result;
    }

}
