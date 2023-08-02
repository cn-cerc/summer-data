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
        switch (symbol) {
        case "=": {
            return target.getLong() == value;
        }
        case ">": {
            return target.getLong() > value;
        }
        case ">=": {
            return target.getLong() >= value;
        }
        case "<": {
            return target.getLong() < value;
        }
        case "<=": {
            return target.getLong() <= value;
        }
        case "<>":
        case "!=": {
            return target.getLong() != value;
        }
        default: {
            throw new RuntimeException(String.format("未匹配到该操作符：%s ！", symbol));
        }
        }
    }

    @Override
    public String field() {
        return field;
    }

}
