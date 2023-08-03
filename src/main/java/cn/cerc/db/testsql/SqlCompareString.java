package cn.cerc.db.testsql;

import java.util.Arrays;

import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.Utils;
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
    public boolean pass(Variant target) {
        String str = target.getString();
        Datetime targetTime = null;
        Datetime valueTime = null;
        if (!Arrays.asList("=", "!=", "<>", "is null", "is not null").contains(symbol)) {
            if (Utils.isEmpty(str)) {
                return false;
            }
            String regex = "\\d{4}-\\d{2}-\\d{2}";
            if (str.length() < 10 || !str.substring(0, 10).matches(regex)) {
                throw new RuntimeException(String.format("暂时只支持DateTime形式比较大小"));
            } else {
                targetTime = new Datetime(str);
                valueTime = new Datetime(value);
            }
        }
        switch (symbol) {

        case "=": {
            return str.equals(value);
        }
        case ">": {
            return targetTime.after(valueTime);
        }
        case ">=": {
            return !targetTime.before(valueTime);
        }
        case "<": {
            return targetTime.before(valueTime);
        }
        case "<=": {
            return !targetTime.after(valueTime);
        }
        case "<>":
        case "!=": {
            return !str.equals(value);
        }
        case "is null": {
            return Utils.isEmpty(str);
        }
        case "is not null": {
            return !Utils.isEmpty(str);
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
