package cn.cerc.db.testsql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.Utils;

public class SqlWhereFilter {
    private List<List<SqlCompareImpl>> list;
    private String sql;

    public SqlWhereFilter(String sql) {
        this.sql = sql;
        this.list = this.decode(this.sql);
    }

    public boolean pass(DataRow row) {
        for (var keys : list) {
            if (keys.stream().filter(key -> key.pass(row.bind(key.field()))).count() == keys.size())
                return true;
        }
        return false;
    }

    protected List<List<SqlCompareImpl>> decode(String sql) {
        LinkedList<List<SqlCompareImpl>> list = new LinkedList<>();
        int offset = sql.toLowerCase().indexOf("where");
        if (offset > -1) {
            int endIndex = sql.toLowerCase().indexOf("order by");
            String[] items;
            if (endIndex > -1) {
                items = sql.substring(offset + 5, endIndex).split(" or ");
            } else {
                items = sql.substring(offset + 5).split(" or ");
            }
            for (String item1 : items) {
                list.add(new ArrayList<>());
                for (var item2 : item1.split(" and ")) {
                    if (item2.split(">=").length == 2) {
                        setCondition(list, item2, ">=");
                    } else if (item2.split("<=").length == 2) {
                        setCondition(list, item2, "<=");
                    } else if (item2.split("<>").length == 2) {
                        setCondition(list, item2, "<>");
                    } else if (item2.split("!=").length == 2) {
                        setCondition(list, item2, "!=");
                    } else if (item2.split("=").length == 2) {
                        setCondition(list, item2, "=");
                    } else if (item2.split(">").length == 2) {
                        setCondition(list, item2, ">");
                    } else if (item2.split("<").length == 2) {
                        setCondition(list, item2, "<");
                    } else if (item2.toLowerCase().contains("is null")) {
                        setCondition(list, item2, "is null");
                    } else if (item2.toLowerCase().contains("is not null")) {
                        setCondition(list, item2, "is not null");
                    } else if (item2.split("like").length == 2) {
                        String[] tmp = item2.split("like");
                        String field = tmp[0].trim();
                        String value = tmp[1].trim();
                        if (value.startsWith("'") && value.endsWith("'")) {
                            list.getLast().add(new SqlCompareLike(field, value.substring(1, value.length() - 1)));
                        } else {
                            throw new RuntimeException(String.format("模糊查询条件：%s 必须为字符串", item2));
                        }
                    } else if (item2.split("in").length == 2) {
                        String[] tmp = item2.split("in");
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
                            list.getLast().add(new SqlCompareIn(field, values));
                        } else {
                            throw new RuntimeException(String.format("in查询条件：%s 必须有带有()", item2));
                        }
                    } else {
                        throw new RuntimeException(String.format("暂不支持的查询条件：%s", item2));
                    }
                }
            }
        }
        return list;
    }

    private void setCondition(LinkedList<List<SqlCompareImpl>> list, String item, String symbol) {
        String[] tmp = item.split(symbol);
        String field = tmp[0].trim();
        String value = "";
        if (!Arrays.asList("is null", "is not null").contains(symbol)) {
            value = tmp[1].trim();
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            list.getLast().add(new SqlCompareString(field, symbol, value.substring(1, value.length() - 1)));
        } else if (Utils.isNumeric(value)) {
            list.getLast().add(new SqlCompareNumeric(field, symbol, Double.parseDouble(value)));
        } else {
            list.getLast().add(new SqlCompareString(field, symbol, value));
        }
    }

}
