package cn.cerc.db.testsql;

import java.util.ArrayList;

import cn.cerc.db.core.Variant;

public class SqlCompareIn implements SqlCompareImpl {
    private String field;
    private ArrayList<String> values;

    public SqlCompareIn(String field, ArrayList<String> values) {
        this.field = field;
        this.values = values;
    }

    @Override
    public boolean pass(Variant target) {
        String targetStr = target.getString();
        return values.contains(targetStr);
    }

    @Override
    public String field() {
        return field;
    }

}
