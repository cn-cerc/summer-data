package cn.cerc.db.testsql;

import cn.cerc.db.core.Variant;

public class SqlCompareLike implements SqlCompareImpl {
    private String field;
    private String value;

    public SqlCompareLike(String field, String value) {
        this.field = field;
        this.value = value;
    }

    @Override
    public boolean pass(Variant target) {
        String targetStr = target.getString();
        if (value.startsWith("%")) {
            String processedStr = value.substring(1);
            if (value.endsWith("%")) {
                // like %s%
                processedStr = processedStr.substring(0, processedStr.length() - 1);
                return targetStr.contains(processedStr);
            } else {
                // like %s
                return targetStr.endsWith(processedStr);
            }
        } else {
            if (value.endsWith("%")) {
                // like s%
                String processedStr = value.substring(0, value.length() - 1);
                return targetStr.startsWith(processedStr);
            } else {
                // like s 相当于 =
                return targetStr.equals(value);
            }
        }
    }

    @Override
    public String field() {
        return field;
    }

}
