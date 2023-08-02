package cn.cerc.db.testsql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.Utils;

public class SqlWhereFilter {
    private List<SqlCompareImpl> list;
    private String sql;

    public SqlWhereFilter(String sql) {
        this.sql = sql;
        this.list = this.decode(this.sql);
    }

    public boolean pass(DataRow row) {
        var pass = true;
        for (var key : list) {
            if (!key.pass(row.bind(key.field())))
                pass = false;
        }
        return pass;
    }

    protected List<SqlCompareImpl> decode(String sql) {
        List<SqlCompareImpl> list = new ArrayList<>();
        int offset = sql.toLowerCase().indexOf("where");
        if (offset > -1) {
            int endIndex = sql.toLowerCase().indexOf("order by");
            String[] items;
            if (endIndex > -1) {
                items = sql.substring(offset + 5, endIndex).split(" and ");
            } else {
                items = sql.substring(offset + 5).split(" and ");
            }
            for (String item : items) {
                if (item.split(">=").length == 2) {
                    setCondition(list, item, ">=");
                } else if (item.split("<=").length == 2) {
                    setCondition(list, item, "<=");
                } else if (item.split("<>").length == 2) {
                    setCondition(list, item, "<>");
                } else if (item.split("!=").length == 2) {
                    setCondition(list, item, "!=");
                } else if (item.split("=").length == 2) {
                    setCondition(list, item, "=");
                } else if (item.split(">").length == 2) {
                    setCondition(list, item, ">");
                } else if (item.split("<").length == 2) {
                    setCondition(list, item, "<");
                } else if (item.toLowerCase().contains("is null")) {
                    setCondition(list, item, "is null");
                } else if (item.toLowerCase().contains("is not null")) {
                    setCondition(list, item, "is not null");
                } else if (item.split("like").length == 2) {
                    String[] tmp = item.split("like");
                    String field = tmp[0].trim();
                    String value = tmp[1].trim();
                    if (value.startsWith("'") && value.endsWith("'")) {
                        list.add(new SqlCompareLike(field, value.substring(1, value.length() - 1)));
                    } else {
                        throw new RuntimeException(String.format("模糊查询条件：%s 必须为字符串", item));
                    }
                } else if (item.split("in").length == 2) {
                    String[] tmp = item.split("in");
                    String field = tmp[0].trim();
                    String value = tmp[1].trim();
                    if (value.startsWith("(") && value.endsWith(")")) {
                        var values = new ArrayList<String>();
                        for (String str : value.substring(1, value.length() - 1).split(",")) {
                            str = str.trim();
                            if (str.startsWith("'") && str.endsWith("'")) {
                                values.add((str.substring(1, str.length() - 1)).trim());
                            } else {
                                values.add(str);
                            }
                        }
                        list.add(new SqlCompareIn(field, values));
                    } else {
                        throw new RuntimeException(String.format("in查询条件：%s 必须有带有()", item));
                    }
                } else {
                    throw new RuntimeException(String.format("暂不支持的查询条件：%s", item));
                }
            }
        }
        return list;
    }

    private void setCondition(List<SqlCompareImpl> list, String item, String symbol) {
        String[] tmp = item.split(symbol);
        String field = tmp[0].trim();
        String value = "";
        if (!Arrays.asList("is null", "is not null").contains(symbol)) {
            value = tmp[1].trim();
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            list.add(new SqlCompareString(field, symbol, value.substring(1, value.length() - 1)));
        } else if (Utils.isNumeric(value)) {
            list.add(new SqlCompareNumeric(field, symbol, Double.parseDouble(value)));
        } else {
            list.add(new SqlCompareString(field, symbol, value));
        }
    }

}
