package cn.cerc.db.testsql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.Utils;

public class SqlWhereFilter {
    private List<SqlCompareImpl> andList;
    private List<SqlCompareImpl> orList;
    private String sql;

    public SqlWhereFilter(String sql) {
        this.sql = sql;
        this.orList = new ArrayList<>();
        this.andList = new ArrayList<>();
        this.decode(this.sql);
    }

    public boolean pass(DataRow row) {
        var pass = true;

        for (var key : orList) {
            if (key.pass(row.bind(key.field())))
                return true;
        }

        for (var key : andList) {
            if (!key.pass(row.bind(key.field())))
                pass = false;
        }
        return pass;
    }

    protected String[] processBracket(String sql) {
        Stack<String> stack = new Stack<>();
        int index = 0;
        while (index < sql.length()) {
            if (sql.charAt(index) == '(')
                stack.push("(");

        }
        return null;
    }

    protected void decode(String sql) {
        int offset = sql.toLowerCase().indexOf("where");
        if (offset > -1) {
            int endIndex = sql.toLowerCase().indexOf("order by");
            String whereStr;
            if (endIndex > -1) {
                whereStr = sql.substring(offset + 5, endIndex);
            } else {
                whereStr = sql.substring(offset + 5);
            }
            List<String> andItems = new ArrayList<>();
            List<String> orItems = new ArrayList<>();
            String[] orArray = whereStr.split("or ");
            if (orArray.length > 1) {
                String first = orArray[0];
                if (first.contains("and")) {
                    String[] temp = first.split("and");
                    for (String s : temp) {
                        andItems.add(s);
                    }
                } else {
                    andItems.add(first);
                }
                for (int i = 1; i < orArray.length; i++) {
                    String str = orArray[i];
                    if (str.contains("and")) {
                        String[] temp = str.split("and");
                        orItems.add(temp[0]);
                        for (int j = 1; j < temp.length; j++) {
                            andItems.add(temp[j]);
                        }
                    } else {
                        orItems.add(str);
                    }
                }
            } else {
                for (String temp : whereStr.split("and")) {
                    andItems.add(temp);
                }
            }
            addList(orList, orItems);
            addList(andList, andItems);
        }
    }

    // 判断单元是否通过
    private boolean unitTest(String sql) {
        if (!sql.contains("(")) {

        }
        return false;
    }

    // 选择对应比较器
    private boolean selectCompare(String item, DataRow row) {
        if (item.split(">=").length == 2) {
            return checkUnit(item, ">=", row);
        } else if (item.split("<=").length == 2) {
            return checkUnit(item, "<=", row);
        } else if (item.split("<>").length == 2) {
            return checkUnit(item, "<>", row);
        } else if (item.split("!=").length == 2) {
            return checkUnit(item, "!=", row);
        } else if (item.split("=").length == 2) {
            return checkUnit(item, "=", row);
        } else if (item.split(">").length == 2) {
            return checkUnit(item, ">", row);
        } else if (item.split("<").length == 2) {
            return checkUnit(item, "<", row);
        } else if (item.toLowerCase().contains("is null")) {
            return checkUnit(item, "is null", row);
        } else if (item.toLowerCase().contains("is not null")) {
            return checkUnit(item, "is not null", row);
        } else if (item.split("like").length == 2) {
            String[] tmp = item.split("like");
            String field = tmp[0].trim();
            String value = tmp[1].trim();
            if (value.startsWith("'") && value.endsWith("'")) {
                SqlCompareLike like = new SqlCompareLike(field, value.substring(1, value.length() - 1));
                return like.pass(row.bind(like.field()));
            } else {
                throw new RuntimeException(String.format("模糊查询条件：%s 必须为字符串", item));
            }
        } else if (item.split("in").length == 2 || item.split("in").length == 2) {
            String symbol = item.contains("not") ? "not in" : "in";
            String[] tmp = item.split(symbol);
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
                SqlCompareIn in = new SqlCompareIn(field, values, symbol);
                return in.pass(row.bind(in.field()));
            } else {
                throw new RuntimeException(String.format("in查询条件：%s 必须有带有()", item));
            }
        } else {
            throw new RuntimeException(String.format("暂不支持的查询条件：%s", item));
        }

    }

    private void addList(List<SqlCompareImpl> list, List<String> items) {
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
            } else if (item.split("in").length == 2 || item.split("in").length == 2) {
                String symbol = item.contains("not") ? "not in" : "in";
                String[] tmp = item.split(symbol);
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
                    list.add(new SqlCompareIn(field, values, symbol));
                } else {
                    throw new RuntimeException(String.format("in查询条件：%s 必须有带有()", item));
                }
            } else {
                throw new RuntimeException(String.format("暂不支持的查询条件：%s", item));
            }
        }
    }

    private void setCondition(List<SqlCompareImpl> list, String item, String symbol) {
        String[] tmp = item.split(symbol);
        String field = tmp[0].trim();
        String value = "";
        if (!Arrays.asList("is null", "is not null").contains(symbol)) {
            value = tmp[1].trim();
        }
        if (value.contains("limit ")) {
            value = value.substring(0, value.indexOf("limit ")).trim();
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            list.add(new SqlCompareString(field, symbol, value.substring(1, value.length() - 1)));
        } else if (Utils.isNumeric(value)) {
            list.add(new SqlCompareNumeric(field, symbol, Double.parseDouble(value)));
        } else {
            list.add(new SqlCompareString(field, symbol, value));
        }
    }

    private boolean checkUnit(String item, String symbol, DataRow row) {
        String[] tmp = item.split(symbol);
        String field = tmp[0].trim();
        String value = "";
        if (!Arrays.asList("is null", "is not null").contains(symbol)) {
            value = tmp[1].trim();
        }
        if (value.contains("limit ")) {
            value = value.substring(0, value.indexOf("limit ")).trim();
        }
        SqlCompareImpl compare;
        if (value.startsWith("'") && value.endsWith("'")) {
            compare = new SqlCompareString(field, symbol, value.substring(1, value.length() - 1));
        } else if (Utils.isNumeric(value)) {
            compare = new SqlCompareNumeric(field, symbol, Double.parseDouble(value));
        } else {
            compare = new SqlCompareString(field, symbol, value);
        }
        return compare.pass(row.bind(compare.field()));
    }

}
