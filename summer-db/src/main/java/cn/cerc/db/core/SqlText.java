package cn.cerc.db.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import cn.cerc.db.SummerDB;

public class SqlText implements Serializable {
    private static final long serialVersionUID = 5202024253700579642L;
    private static final ClassResource res = new ClassResource(SqlText.class, SummerDB.ID);
    // 从数据库每次加载的最大笔数
    public static final int MAX_RECORDS = 50000;
    //
    public static final int PUBLIC = 1;
    public static final int PRIVATE = 2;
    public static final int PROTECTED = 4;

    private int maximum = MAX_RECORDS;
    private int offset = 0;
    // sql 指令
    private String text;
    private ClassData classData;
    private SqlServerType sqlServerType;
    private Class<?> clazz;

    public SqlText(SqlServerType sqlServerType) {
        super();
        this.sqlServerType = sqlServerType;
    }

    public SqlText(Class<?> clazz) {
        super();
        this.clazz = clazz;
        SqlServer server = clazz.getAnnotation(SqlServer.class);
        this.sqlServerType = (server != null) ? server.type() : SqlServerType.Mysql;
        classData = ClassFactory.get(clazz);
        this.text = classData.getSelect();
    }

    public SqlText(String format, Object... args) {
        add(format, args);
    }

    public SqlText add(String text) {
        if (text == null) {
            throw new RuntimeException("sql not is null");
        }
        if (this.text == null) {
            this.text = text;
        } else {
            this.text = this.text + " " + text;
        }
        return this;
    }

    public SqlText add(String format, Object... args) {
        ArrayList<Object> items = new ArrayList<>();
        for (Object arg : args) {
            if (arg instanceof String) {
                items.add(Utils.safeString((String) arg));
            } else {
                items.add(arg);
            }
        }
        return this.add(String.format(format, items.toArray()));
    }

    public String text() {
        return text;
    }

    public String getSelect() {
        String sql = this.text;
        if (sql == null || "".equals(sql)) {
            throw new RuntimeException("SqlText.Text is null ！");
        }

        sql = sql + String.format(" limit %d,%d", this.offset, this.maximum);
        return sql;
    }

    public SqlText clear() {
        this.text = null;
        return this;
    }

    public int offset() {
        return offset;
    }

    @Deprecated
    public final int getOffset() {
        return offset();
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Deprecated
    public final String getText() {
        return text();
    }

    public String getCommand() {
        String sql = this.text();
        if (sql == null || "".equals(sql)) {
            throw new RuntimeException("SqlText.text is null ！");
        }

        if (sql.contains("call ") || sql.contains("show")) {
            return sql;
        }

        if (sqlServerType == SqlServerType.Mysql) {
            if (this.offset > 0) {
                if (this.maximum < 0) {
                    sql = sql + String.format(" limit %d,%d", this.offset, MAX_RECORDS + 1);
                } else {
                    sql = sql + String.format(" limit %d,%d", this.offset, this.maximum + 1);
                }
            } else if (this.maximum == MAX_RECORDS) {
                sql = sql + String.format(" limit %d", this.maximum + 2);
            } else if (this.maximum > -1) {
                sql = sql + String.format(" limit %d", this.maximum + 1);
            }
        }
        return sql;
    }

    public int maximum() {
        return maximum;
    }

    @Deprecated
    public final int getMaximum() {
        return maximum();
    }

    public void setMaximum(int maximum) {
        if (maximum > MAX_RECORDS) {
            throw new RuntimeException(String.format(res.getString(1, "本次请求的记录数超出了系统最大笔数为 %d 的限制！"), MAX_RECORDS));
        }
        this.maximum = maximum;
    }

    public String getTableId() {
        return classData != null ? classData.getTableId() : null;
    }

    @Deprecated // 请改使用 add(whereText).getText()
    public String getWhere(String whereText) {
        return add(whereText).getText();
    }

    @Deprecated // 请改使用 addWhereKeys(values).getText()
    public final String getWhereKeys(Object... values) {
        return addWhereKeys(values).getText();
    }

    @Deprecated
    public final SqlText addWhereKeys(Object... values) {
        if (values.length == 0) {
            throw new RuntimeException("values is null");
        }

        if (classData == null) {
            throw new RuntimeException("classData is null");
        }

        StringBuffer sb = new StringBuffer();
        List<String> idList = classData.getSearchKeys();
        if (idList.size() == 0) {
            throw new RuntimeException("id is null");
        }

        if (idList.size() != values.length) {
            throw new RuntimeException(String.format("ids.size(%s) != values.size(%s)", idList.size(), values.length));
        }

        int i = 0;
        int count = idList.size();
        if (count > 0) {
            sb.append("where");
        }
        for (String fieldCode : idList) {
            Object value = values[i];
            sb.append(i > 0 ? " and " : " ");
            if (value == null) {
                sb.append(String.format("%s is null", fieldCode));
            }
            if (value instanceof String) {
                sb.append(String.format("%s='%s'", fieldCode, Utils.safeString((String) value)));
            } else {
                sb.append(String.format("%s='%s'", fieldCode, value));
            }
            i++;
        }

        return add(sb.toString());
    }

    public ClassData getClassData() {
        return classData;
    }

    @Deprecated
    public boolean isSupportMssql() {
        return sqlServerType == SqlServerType.Mysql;
    }

    // 根据 sql 获取数据库表名
    public static String findTableName(String sql) {
        // 函数、存储过程则不获取table
        if (sql.startsWith("call")) {
            return null;
        }
        String result = null;
        String[] items = sql.split("[ \r\n]");
        for (int i = 0; i < items.length; i++) {
            if (items[i].toLowerCase().contains("from")) {
                // 如果取到form后 下一个记录为数据库表名
                while (items[i + 1] == null || "".equals(items[i + 1].trim())) {
                    // 防止取到空值
                    i++;
                }
                result = items[++i]; // 获取数据库表名
                break;
            }
        }

        if (result == null) {
            throw new RuntimeException("sql command error");
        }

        return result;
    }

    public SqlServerType getSqlServerType() {
        return sqlServerType;
    }

    public SqlWhere addWhere() {
        return new SqlWhere().setSqlText(this);
    }

    public SqlWhere addWhere(DataRow dataRow) {
        return new SqlWhere().setSqlText(this).setDataRow(dataRow);
    }

    public SqlText addSelectDefault() {
        if (this.clazz == null)
            throw new IllegalArgumentException("clazz is null");
        this.add("select * from %s", Utils.findTable(clazz));
        return this;
    }

}
