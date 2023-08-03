package cn.cerc.db.testsql;

import java.util.ArrayList;

import cn.cerc.db.core.Variant;

public class SqlCompareIn implements SqlCompareImpl {
    private String field;
    private ArrayList<String> values;
    private String symbol;

    public SqlCompareIn(String field, ArrayList<String> values, String symbol) {
        this.field = field;
        this.values = values;
        this.symbol = symbol;
    }

    @Override
    public boolean pass(Variant target) {
        String targetStr = target.getString();
        return symbol.contains("not") ? !values.contains(targetStr) : values.contains(targetStr);
    }

    @Override
    public String field() {
        return field;
    }

}
