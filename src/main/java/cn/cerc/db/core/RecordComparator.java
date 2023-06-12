package cn.cerc.db.core;

import java.util.Comparator;

public class RecordComparator implements Comparator<DataRow> {
    private String[] fields;

    public RecordComparator(String[] fields) {
        this.fields = fields;
    }

    @Override
    public int compare(DataRow o1, DataRow o2) {
        long tmp = 0;
        for (String item : fields) {
            if (item == null || "".equals(item)) {
                throw new RuntimeException("sort field is empty");
            }
            String[] params = item.split(" ");
            String field = params[0];

            Object v1 = o1.getValue(field);
            Object v2 = o2.getValue(field);
            if (v1 instanceof Integer && v2 instanceof Integer) {
                tmp = o1.getInt(field) - o2.getInt(field);
            } else if (v1 instanceof Float || v1 instanceof Double || v1 instanceof Long || v2 instanceof Float
                    || v2 instanceof Double || v2 instanceof Long) {
                double df = o1.getDouble(field) - o2.getDouble(field);
                if (df == 0)
                    tmp = 0;
                else
                    tmp = df > 0 ? 1 : -1;
            } else if (v1 instanceof Datetime) {
                tmp = o1.getDatetime(field).compareTo(o2.getDatetime(field));
            } else {
                tmp = o1.getString(field).compareTo(o2.getString(field));
            }

            if (tmp != 0) {
                if (params.length == 1 || "ASC".equalsIgnoreCase(params[1])) {
                    return tmp > 0 ? 1 : -1;
                } else if ("DESC".equalsIgnoreCase(params[1])) {
                    return tmp > 0 ? -1 : 1;
                } else {
                    throw new RuntimeException(String.format("not support [%s] sort mode", params[1]));
                }
            }
        }
        return 0;
    }
}
