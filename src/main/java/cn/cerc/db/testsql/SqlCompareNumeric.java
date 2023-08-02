package cn.cerc.db.testsql;

import cn.cerc.db.core.Variant;

public class SqlCompareNumeric implements SqlCompareImpl {
    private String field;
    private String symbol;
    private double value;

    public SqlCompareNumeric(String field, String symbol, double value) {
        this.field = field;
        this.symbol = symbol;
        this.value = value;
    }

    @Override
    public boolean pass(Variant target) {
        return value == target.getLong();
    }

    @Override
    public String field() {
        return field;
    }

}
