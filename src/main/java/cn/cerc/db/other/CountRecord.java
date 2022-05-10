package cn.cerc.db.other;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.DataSet;

public class CountRecord {
    private DataSet dataSet;
    private Map<String, Integer> groups = new HashMap<>();

    public CountRecord(DataSet dataSet) {
        this.dataSet = dataSet;
    }

    public CountRecord run(CountRecordInterface count) {
        for (DataRow rs : dataSet) {
            String group = count.getGroup(rs);
            Integer value = groups.get(group);
            if (value == null) {
                groups.put(group, 1);
            } else {
                groups.put(group, value + 1);
            }
        }
        return this;
    }

    public int getCount(String group) {
        Integer value = groups.get(group);
        return value == null ? 0 : value;
    }

    public Set<String> getGroups() {
        return groups.keySet();
    }
}
