package cn.cerc.db.testsql;

import cn.cerc.db.core.Variant;

public class SqlCompareString implements SqlCompareImpl {
    private String field;
    private String symbol;
    private String value;

    public SqlCompareString(String field, String symbol, String value) {
        this.field = field;
        this.symbol = symbol;
        this.value = value;
    }

    @Override
    public boolean pass(Variant targe) {
        return false;
    }

    @Override
    public String field() {
        return field;
    }

}
