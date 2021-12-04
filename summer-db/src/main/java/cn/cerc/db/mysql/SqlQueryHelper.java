package cn.cerc.db.mysql;

import static cn.cerc.core.Utils.safeString;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cn.cerc.core.Datetime;
import cn.cerc.core.ISession;
import cn.cerc.core.Utils;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlQuery;

/**
 * 用于组合生成select指令，便于多条件查询编写
 *
 * @author 张弓
 */
public class SqlQueryHelper<T extends SqlQuery> implements IHandle {
    public static final String vbCrLf = "\r\n";
    protected T dataSet;
    protected List<String> where = new ArrayList<>();
    private List<String> content = new ArrayList<>();
    private String order;
    private String group;
    private ISession session;

    public SqlQueryHelper(ISession session) {
        super();
        this.session = session;
    }

    public SqlQueryHelper(T query) {
        super();
        this.session = query.getSession();
        this.dataSet = query;
    }

    /**
     * 支持多个组合模糊查询条件组合查询
     * <p>
     * 一个查询文本对应一个字段组合的 List
     * <p>
     * 生成 SQL 的指令如下：
     * <p>
     * and ( CusCode_ like '%05559255%' or SalesCode_ like '%05559255%' or AppUser_
     * like '%05559255%' or UpdateUser_ like '%05559255%' or Address_ like
     * '%05559255%' or Mobile_ like '%$i8OknluCnFsW$%' )
     *
     * @param items 数据库语句拼接
     * @return BuildQuery
     */
    public SqlQueryHelper<T> byLink(Map<String, List<String>> items) {
        if (items == null) {
            return this;
        }
        StringBuilder builder = new StringBuilder();
        for (String k : items.keySet()) {
            List<String> fields = items.get(k);
            String text = "%" + safeString(k).replaceAll("\\*", "") + "%";
            for (String field : fields) {
                builder.append(String.format("%s like '%s'", field, text));
                builder.append(" or ");
            }
        }
        String str = builder.toString();
        str = str.substring(0, str.length() - 3);
        where.add("(" + str + ")");
        return this;
    }

    public SqlQueryHelper<T> byLink(String[] fields, String value) {
        if (value == null || "".equals(value)) {
            return this;
        }
        String str = "";
        String s1 = "%" + safeString(value).replaceAll("\\*", "") + "%";
        for (String sql : fields) {
            str = str + String.format("%s like '%s'", sql, s1);
            str = str + " or ";
        }
        str = str.substring(0, str.length() - 3);
        where.add("(" + str + ")");
        return this;
    }

    public SqlQueryHelper<T> byNull(String field, boolean value) {
        String s = value ? "not null" : "null";
        where.add(String.format("%s is %s", field, s));
        return this;
    }

    public SqlQueryHelper<T> addWhere(String field, String text) {
        String value = safeString(text);
        if ("".equals(value)) {
            return this;
        }
        if ("*".equals(value)) {
            return this;
        }
        if (value.contains("*")) {
            where.add(String.format("%s like '%s'", field, value.replace("*", "%")));
            return this;
        }
        if ("``".equals(value)) {
            where.add(String.format("%s='%s'", field, "`"));
            return this;
        }
        if ("`is null".equals(value)) {
            where.add(String.format("(%s is null or %s='')", field, field));
            return this;
        }
        if (!value.startsWith("`")) {
            where.add(String.format("%s='%s'", field, value));
            return this;
        }
        if ("`=".equals(value.substring(0, 2))) {
            where.add(String.format("%s=%s", field, value.substring(2)));
            return this;
        }
        if ("`!=".equals(value.substring(0, 3)) || "`<>".equals(value.substring(0, 3))) {
            where.add(String.format("%s<>%s", field, value.substring(3)));
            return this;
        }
        return this;
    }

    public SqlQueryHelper<T> addWhere(String field, int value) {
        where.add(String.format("%s=%s", field, value));
        return this;
    }

    public SqlQueryHelper<T> addWhere(String field, double value) {
        where.add(String.format("%s=%s", field, value));
        return this;
    }

    public SqlQueryHelper<T> addWhere(String field, Datetime value) {
        where.add(String.format("%s='%s'", field, value.format("yyyy-MM-dd HH:mm:ss")));
        return this;
    }

    public SqlQueryHelper<T> addWhere(String field, boolean value) {
        int s = value ? 1 : 0;
        where.add(String.format("%s=%s", field, s));
        return this;
    }

    public SqlQueryHelper<T> byBetween(String field, String value1, String value2) {
        where.add(String.format("%s between '%s' and '%s'", field, safeString(value1), safeString(value2)));
        return this;
    }

    public SqlQueryHelper<T> byBetween(String field, int value1, int value2) {
        where.add(String.format("%s between %s and %s", field, value1, value2));
        return this;
    }

    public SqlQueryHelper<T> byBetween(String field, double value1, double value2) {
        where.add(String.format("%s between %s and %s", field, value1, value2));
        return this;
    }

    public SqlQueryHelper<T> byBetween(String field, Datetime value1, Datetime value2) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        where.add(String.format(" %s between '%s' and '%s' ", field, sdf.format(value1.asBaseDate()),
                sdf.format(value2.asBaseDate())));
        return this;
    }

    public SqlQueryHelper<T> byRange(String field, String... values) {
        // where code_ in ("aa","Bb")
        if (values.length > 0) {
            String s = field + " in (";
            for (String val : values) {
                s = s + "'" + safeString(val) + "',";
            }
            s = s.substring(0, s.length() - 1) + ")";
            where.add(s);
        }
        return this;
    }

    public SqlQueryHelper<T> byRange(String field, int[] values) {
        if (values.length > 0) {
            String s = field + " in (";
            for (int sql : values) {
                s = s + sql + ",";
            }
            s = s.substring(0, s.length() - 1) + ")";
            where.add(s);
        }
        return this;
    }

    public SqlQueryHelper<T> byRange(String field, double[] values) {
        if (values.length > 0) {
            String s = field + " in (";
            for (double sql : values) {
                s = s + sql + ",";
            }
            s = s.substring(0, s.length() - 1) + ")";
            where.add(s);
        }
        return this;
    }

    public SqlQueryHelper<T> setSelect(String text) {
        content.clear();
        if (!text.toUpperCase().startsWith("select "))
            throw new RuntimeException("startsWith is not select");
        return addSelect(text);
    }

    public SqlQueryHelper<T> addSelect(String text) {
        String regex = "((\\bselect)|(\\bSelect)|(\\s*select)|(\\s*Select))\\s*(distinct)*\\s+%s";
        if (text.matches(regex)) {
            text = text.replaceFirst("%s", "");
        }
        content.add(text);
        return this;
    }

    public SqlQueryHelper<T> addSelect(String fmtText, Object... args) {
        ArrayList<Object> items = new ArrayList<>();
        for (Object arg : args) {
            if (arg instanceof String) {
                items.add(Utils.safeString((String) arg));
            } else {
                items.add(arg);
            }
        }
        content.add(String.format(fmtText, items.toArray()));
        return this;
    }

    public T dataSet() {
        return this.dataSet;
    }

    public SqlQueryHelper<T> setDataSet(T dataSet) {
        this.dataSet = dataSet;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("[%s]%n", this.getClass().getName()));
        builder.append(String.format("CommandText:%s%n", this.sqlText()));
        return builder.toString();
    }

    public String sqlText() {
        StringBuffer str = new StringBuffer();
        for (String line : content) {
            if (str.length() > 0) {
                str.append(vbCrLf);
            }
            str.append(line);
        }
        if (where.size() > 0) {
            if (str.length() > 0) {
                str.append(vbCrLf);
            }
            str.append("where ");
            for (String sql : where) {
                str.append(sql).append(" and ");
            }
            str.setLength(str.length() - 5);
        }
        if (group != null) {
            str.append(vbCrLf).append(group);
        }
        if (order != null) {
            str.append(vbCrLf).append(order);
        }

        String sqls = str.toString().trim();
        sqls = sqls.replaceAll(" %s ", " ");
        return sqls;
    }

    public T open() {
        return open(false);
    }

    public T openReadonly() {
        return open(true);
    }

    private T open(boolean slaveServer) {
        T ds = dataSet();
        ds.sql().clear();
        ds.add(this.sqlText());
        if (!slaveServer)
            ds.open();
        else
            ds.openReadonly();
        return ds;
    }

    public void clear() {
        content.clear();
        where.clear();
        order = null;
        if (this.dataSet != null) {
            this.dataSet.clear();
        }
    }

    public int offset() {
        return dataSet().sql().offset();
    }

    public SqlQueryHelper<T> setOffset(int offset) {
        dataSet().sql().setOffset(offset);
        return this;
    }

    public int maximum() {
        return dataSet().sql().maximum();
    }

    public SqlQueryHelper<T> setMaximum(int maximum) {
        dataSet().sql().setMaximum(maximum);
        return this;
    }

    public String order() {
        return this.order;
    }

    public SqlQueryHelper<T> setOrder(String order) {
        if (!Utils.isEmpty(order)) {
            if (order.toLowerCase().startsWith("order by "))
                throw new RuntimeException("startsWidth is not order by");
        }
        this.order = order;
        return this;
    }

    public String group() {
        return this.group;
    }

    public SqlQueryHelper<T> setGroup(String group) {
        if (!Utils.isEmpty(order)) {
            if (order.toLowerCase().startsWith("group by "))
                throw new RuntimeException("startsWidth is not group by");
        }
        this.group = group;
        return this;
    }

    @Override
    public ISession getSession() {
        return session;
    }

    @Override
    public void setSession(ISession session) {
        this.session = session;
    }

    /**
     * 增加自定义查询条件，须自行解决注入攻击！
     *
     * @param param 要加入的查询条件
     * @return 返回自身
     */
    public SqlQueryHelper<T> setWhere(String param) {
        where.clear();
        if (!"".equals(param)) {
            where.add("(" + param + ")");
        }
        return this;
    }
}
