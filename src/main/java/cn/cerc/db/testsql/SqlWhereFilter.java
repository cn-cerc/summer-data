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

    private boolean decode(String sql, DataRow row) {
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
            SqlNode root = new SqlNode().setSql(sql);
            buildTree(root);
            return parseTree(root, row);
        }
        return true;
    }

    // 构建sql树
    private void buildTree(SqlNode node) {
        if (isMinimumNode(node)) {
            return;
        }
        String sql = node.getSql();
        if (sql.contains("(")) {
            processBracket(node);
        } else {
            String[] orArray = sql.split("or ");
            if (orArray.length > 1) {
                for (int i = 0; i < orArray.length - 1; i++) {
                    String subOrSql = orArray[i];
                    if (subOrSql.contains("and ")) {
                        String[] subAndArray = subOrSql.split("and ");
                        for (int j = 0; j < subAndArray.length - 1; j++) {
                            node.addSubNode(new SqlNode().setSql(subAndArray[j]).setConj("and"));
                        }
                        node.addSubNode(new SqlNode().setSql(subAndArray[subAndArray.length - 1]));
                    } else {
                        node.addSubNode(new SqlNode().setSql(orArray[i]).setConj("or"));
                    }
                }
                node.addSubNode(new SqlNode().setSql(orArray[orArray.length - 1]));
            } else {
                String[] subAndArray = sql.split("and");
                for (int i = 0; i < subAndArray.length - 1; i++) {
                    node.addSubNode(new SqlNode().setSql(subAndArray[i]).setConj("and"));
                }
                node.addSubNode(new SqlNode().setSql(subAndArray[subAndArray.length - 1]));
            }
        }
        for (SqlNode subNode : node.getSubNodes()) {
            buildTree(subNode);
        }
    }

    // 判断当前结点是否为最小结点
    private boolean isMinimumNode(SqlNode node) {
        // 条件加空格是为了排除如android_,order_等含有and与or的字段
        boolean flag = node.getSql().contains("and ") || node.getSql().contains("or ");
        return !flag;
    }

    // 处理括号
    private void processBracket(SqlNode node) {
        String sql = node.getSql();
        int index = 0;
        // 最后一个右括号的游标
        int lastRight = -1;
        // 处理第一个左括号前的内容
        if (!sql.trim().startsWith("(")) {
            String head = sql.substring(0, sql.indexOf("("));
            // 判断第一个左括号是否为in () 的左括号
            if (head.contains("in ")) {
                String subSql = sql.substring(0, sql.indexOf(")") + 1);
                String conj = getFirstConj(sql.substring(sql.indexOf(")")));
                node.addSubNode(new SqlNode().setSql(subSql).setConj(conj));
                index = sql.indexOf(conj) + conj.length() + 1;
            } else {
                String conj = getLastConj(sql.substring(0, sql.indexOf("(") + 1));
                String subSql = sql.substring(0, sql.lastIndexOf(conj));
                node.addSubNode(new SqlNode().setSql(subSql).setConj(conj));
                index = sql.indexOf(")") + 1;
            }
        }

        // 处理第一个左括号到最后一个右括号中间的内容
        if (index >= sql.length())
            return;
        Stack<Integer> stack = new Stack<>();
        while (index < sql.length()) {
            if (sql.charAt(index) == '(') {
                stack.push(index);
                // 设置stack.size() == 1是为了对于(()())这种情况只对最外层括号进行操作
                if (lastRight != -1 && stack.size() == 1) {
                    // 判断两个括号之间是否有除连词外其他语句 如该种情况:(A) or B and C and (D)
                    String subSql = sql.substring(lastRight + 1, index);
                    if (subSql.trim().length() > 3) {
                        String firstConj = getFirstConj(subSql);
                        String lastConj = getLastConj(subSql);
                        subSql = subSql.substring(subSql.indexOf(firstConj) + firstConj.length() + 1,
                                subSql.lastIndexOf(lastConj));
                        node.addSubNode(new SqlNode().setSql(subSql).setConj(lastConj));
                    }
                }
            }
            if (sql.charAt(index) == ')') {
                if (stack.isEmpty()) {
                    throw new RuntimeException("括号格式不规范！");
                } else {
                    if (stack.size() == 1) {
                        String subStr = sql.substring(stack.peek() + 1, index).trim();
                        // 去除in条件
                        if (subStr.contains("=") || subStr.contains("in ")) {
                            String conj = getFirstConj(sql.substring(index));
                            SqlNode subNode = new SqlNode();
                            if (!Utils.isEmpty(conj)) {
                                subNode.setConj(conj);
                            }
                            node.addSubNode(subNode);
                        }
                    }
                    stack.pop();
                    lastRight = index;
                }
            }
            index++;
        }

        // 处理第最后一个右括号后面的内容
        if (!sql.trim().endsWith(")")) {
            String subSql = sql.substring(sql.lastIndexOf(")") + 1);
            String conj = getFirstConj(subSql);
            subSql = subSql.substring(subSql.indexOf(conj) + conj.length() + 1);
            node.addSubNode(new SqlNode().setSql(subSql));
        }

    }

    // 获取第一个连词
    private String getFirstConj(String sql) {
        if (sql.contains("and ") || sql.contains("or ")) {
            int firstAndIndex = sql.toLowerCase().indexOf("and ");
            int firstOrIndex = sql.toLowerCase().indexOf("or ");
            if (firstAndIndex < 0)
                return "or";
            else if (firstOrIndex < 0)
                return "and";
            else
                return firstAndIndex < firstOrIndex ? "and" : "or";
        }
        return "";
    }

    // 获取最后一个连词
    private String getLastConj(String sql) {
        if (sql.contains("and ") || sql.contains("or ")) {
            int lastAndIndex = sql.toLowerCase().lastIndexOf("and ");
            int lastOrIndex = sql.toLowerCase().indexOf("or ");
            if (lastAndIndex < 0)
                return "or";
            else if (lastOrIndex < 0)
                return "and";
            else
                return lastAndIndex > lastOrIndex ? "and" : "or";
        }
        return "";
    }

    // 解析sql树
    boolean parseTree(SqlNode node, DataRow row) {
        if (isMinimumNode(node)) {
            return selectCompare(node.getSql(), row);
        } else {
            List<SqlNode> subList = node.getSubNodes();
            if (subList.size() == 0) {
                throw new RuntimeException("错误的Sql语句!");
            }
            if (subList.size() == 1) {
                return parseTree(subList.get(0), row);
            }
            boolean result = parseTree(subList.get(subList.size() - 1), row);
            for (int i = subList.size() - 2; i >= 0; i--) {
                if ("and".equals(subList.get(i).getConj())) {
                    result = result && parseTree(subList.get(i - 1), row);
                } else {
                    result = result || parseTree(subList.get(i - 1), row);
                }
            }
            return result;
        }
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
        } else if (item.split("in").length == 2) {
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
