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
    public boolean pass(Variant targe) {
        return false;
    }

    @Override
    public String field() {
        return field;
    }

}
