package cn.cerc.db.testsql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.Utils;

public class SqlWhereFilter {
    private String sql;

    public SqlWhereFilter(String sql) {
        this.sql = sql;
    }

    public boolean pass(DataRow row) {
        return decode(this.sql, row);
    }

    protected boolean decode(String sql, DataRow row) {
        // 筛选出where语句
        int offset = sql.toLowerCase().indexOf("where");
        if (offset > -1) {
            int orderIndex = sql.toLowerCase().indexOf("order by ");
            int limitIndex = sql.toLowerCase().indexOf("limit ");
            String whereStr = sql.substring(offset + 5);
            // limit一定在order by之前
            if (limitIndex > -1) {
                whereStr = sql.substring(offset + 5, limitIndex);
            } else {
                if (orderIndex > -1) {
                    whereStr = sql.substring(offset + 5, orderIndex);
                }
            }
            return parse(whereStr, row);
        }
        return true;
    }

    // 解析语句
    private boolean parse(String sql, DataRow row) {
        // 最小单元:如 Key = value 不包含任何括号以及and和or则直接判断
        if (isMinimum(sql)) {
            return selectCompare(sql, row);
        } else {
            // 处理括号
            boolean flag = sql.contains("(");
            if (flag) {
                String checkStr = sql.substring(sql.indexOf("(") + 1, sql.lastIndexOf(")"));
                // 如果括号中不含有=号,如in (v1, v2, v3)则不进入判断
                flag = checkStr.contains("=");
            }
            if (flag) {
                Stack<Integer> stack = new Stack<>();
                int index = 0;
                int lastRight = -1;
                List<String> items = new ArrayList<>();
                List<String> linkTypes = new ArrayList<>();
                // 处理第一个左括号外的非括号内容 如 A and (B)
                if (!sql.trim().startsWith("(")) {
                    // 去除类似该种情况: key in (v1,v2) and
                    int rightIndex = sql.indexOf("(");
                    String contain = sql.substring(sql.indexOf("(") + 1, sql.indexOf(")"));
                    if (!contain.contains("="))
                        rightIndex = sql.indexOf("(", sql.indexOf(")"));
                    String head = sql.substring(0, rightIndex);
                    int lastAndIndex = head.toLowerCase().lastIndexOf("and ");
                    int lastOrIndex = head.toLowerCase().lastIndexOf("or ");
                    if (lastAndIndex > lastOrIndex) {
                        linkTypes.add("and");
                        items.add(head.substring(0, lastAndIndex).trim());
                        index = lastAndIndex + 4;
                    } else if (lastAndIndex < lastOrIndex) {
                        linkTypes.add("or");
                        items.add(head.substring(0, lastOrIndex).trim());
                        index = lastOrIndex + 3;
                    } else {
                        throw new RuntimeException(String.format("错误的或暂不支持的sql语句，请检查语句：\" %s \"", sql));
                    }
                }

                while (index < sql.length()) {
                    if (sql.charAt(index) == '(') {
                        stack.push(index);
                        if (lastRight != -1 && stack.size() == 1) {
                            // 判断两个括号之间是否还有其他语句 如该种情况:(A) or B and C and (D)
                            String middle = sql.substring(lastRight + 1, index);
                            if (middle.contains("and ")) {
                                if (middle.contains("or ")) {
                                    int firstAndIndex = middle.toLowerCase().indexOf("and ");
                                    int firstOrIndex = middle.toLowerCase().indexOf("or ");
                                    int lastAndIndex = middle.toLowerCase().lastIndexOf("and ");
                                    int lastOrIndex = middle.toLowerCase().lastIndexOf("or ");
                                    if (firstAndIndex < firstOrIndex) {
                                        linkTypes.add("and");
                                        items.add(
                                                middle.substring(firstAndIndex + 4, Math.max(lastAndIndex, lastOrIndex))
                                                        .trim());
                                    } else {
                                        linkTypes.add("or");
                                        items.add(
                                                middle.substring(firstOrIndex + 3, Math.max(lastAndIndex, lastOrIndex))
                                                        .trim());
                                    }
                                    if (lastAndIndex > lastOrIndex)
                                        linkTypes.add("and");
                                    else
                                        linkTypes.add("or");
                                } else {
                                    int firstAndIndex = middle.toLowerCase().indexOf("and ");
                                    int lastAndIndex = middle.toLowerCase().lastIndexOf("and ");
                                    if (firstAndIndex != lastAndIndex) {
                                        items.add(middle.substring(firstAndIndex + 4, lastAndIndex).trim());
                                        linkTypes.add("and");
                                    } else {
                                        if (middle.contains("in")) {
                                            index = sql.indexOf(")", index);
                                            items.add(sql.substring(lastRight + 5, index + 1));
                                        }
                                    }
                                    linkTypes.add("and");
                                }
                            } else if (middle.contains("or ")) {
                                int firstOrIndex = middle.toLowerCase().indexOf("or ");
                                int lastOrIndex = middle.toLowerCase().lastIndexOf("or ");
                                if (firstOrIndex != lastOrIndex) {
                                    items.add(middle.substring(firstOrIndex + 3, lastOrIndex).trim());
                                    linkTypes.add("or");
                                } else {
                                    if (middle.contains("in")) {
                                        index = sql.indexOf(")", index);
                                        items.add(sql.substring(lastRight + 5, index + 1));
                                    }
                                }
                                linkTypes.add("or");
                            }
                            lastRight = -1;
                        }
                    }
                    if (sql.charAt(index) == ')') {
                        if (stack.isEmpty()) {
                            throw new RuntimeException("括号格式不规范！");
                        } else {
                            if (stack.size() == 1) {
                                // 去除in条件
                                String subStr = sql.substring(stack.pop() + 1, index).trim();
                                if (subStr.contains("=") || subStr.contains("in "))
                                    items.add(subStr);
                            } else {
                                stack.pop();
                            }
                            lastRight = index;
                        }
                    }
                    index++;
                }
                // 处理最后一个右括号外的非括号内容 如 (A) and B
                if (!sql.trim().endsWith(")")) {
                    int lastAndIndex = sql.toLowerCase().indexOf("and ", sql.lastIndexOf(")"));
                    int lastOrIndex = sql.toLowerCase().indexOf("or ", sql.lastIndexOf(")"));
                    String lastPart = null;
                    if (lastAndIndex != -1) {
                        if (lastOrIndex != -1) {
                            if (lastAndIndex < lastOrIndex) {
                                // 取距最后一个括号最近的值
                                linkTypes.add("and");
                                lastPart = sql.substring(lastAndIndex + 4);
                            } else {
                                linkTypes.add("or");
                                lastPart = sql.substring(lastOrIndex + 3);
                            }
                        } else {
                            linkTypes.add("and");
                            lastPart = sql.substring(lastAndIndex + 4);
                        }
                    } else {
                        if (lastOrIndex != -1) {
                            linkTypes.add("or");
                            lastPart = sql.substring(lastOrIndex + 3);
                        }
                    }
                    if (Utils.isEmpty(lastPart))
                        throw new RuntimeException(String.format("错误的或暂不支持的sql语句，请检查语句：\" %s \"", sql));
                    else
                        items.add(lastPart.trim());
                }
                boolean result = parse(items.get(0), row);
                for (int i = 0; i < linkTypes.size(); i++) {
                    if ("and".equals(linkTypes.get(i))) {
                        result = result && parse(items.get(i + 1), row);
                    } else {
                        result = result || parse(items.get(i + 1), row);
                    }
                }
                return result;
            } else {
                // 处理 and or 语句
                List<String> andItems = new ArrayList<>();
                List<String> orItems = new ArrayList<>();
                String[] orArray = sql.split("or ");
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
                    for (String temp : sql.split("and")) {
                        andItems.add(temp);
                    }
                }
                for (String temp : orItems) {
                    // or 条件通过一条就直接全部通过
                    if (parse(temp, row)) {
                        return true;
                    }
                }
                for (String temp : andItems) {
                    // and 条件一条不通过就全部不通过
                    if (!parse(temp, row)) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    // 判断当前单元是否为最小单元
    private boolean isMinimum(String sql) {
        // 条件加空格是为了排除如android_,order_等含有and与or的字段
        boolean flag = sql.contains("and ") || sql.contains("or ");
        return !flag;
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

    // 对最小单元进行判断
    private boolean checkUnit(String item, String symbol, DataRow row) {
        String[] tmp = item.split(symbol);
        String field = tmp[0].trim();
        String value = "";
        if (!Arrays.asList("is null", "is not null").contains(symbol)) {
            value = tmp[1].trim();
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
