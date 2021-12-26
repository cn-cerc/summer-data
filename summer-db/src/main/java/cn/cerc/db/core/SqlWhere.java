package cn.cerc.db.core;

import java.util.Objects;

import cn.cerc.core.DataRow;
import cn.cerc.core.Datetime;
import cn.cerc.core.SqlText;
import cn.cerc.core.Utils;

public class SqlWhere {
    private OperationEnum operation = OperationEnum.And;
    private StringBuffer sb = new StringBuffer();
    private SqlText sqlText;
    private int size; // 统计加入了多少个条件

    public enum OperationEnum {
        And, Or
    }

    public enum LinkOptionEnum {
        Right, Left, All
    }

    public SqlWhere() {
        super();
    }

    public SqlWhere(SqlText sqlText) {
        this.sqlText = sqlText;
    }

    /**
     * 设置接下来的条件为and
     * 
     * @return this
     */
    public final SqlWhere and() {
        this.operation = OperationEnum.And;
        return this;
    }

    /**
     * 设置接下来的条件为or
     * 
     * @return this
     */
    public final SqlWhere or() {
        this.operation = OperationEnum.Or;
        return this;
    }

    /**
     * 设置条件：相等
     * 
     * @param field 数据表字段名
     * @param value 条件值
     * @return this
     */
    public SqlWhere eq(String field, Object value) {
        return this.appendField(field, value, "=");
    }

    /**
     * 设置条件：不相等
     * 
     * @param field 数据表字段名
     * @param value 条件值
     * @return this
     */
    public SqlWhere neq(String field, Object value) {
        return this.appendField(field, value, "<>");
    }

    /**
     * 设置条件：大于
     * 
     * @param field 数据表字段名
     * @param value 条件值
     * @return this
     */
    public SqlWhere gt(String field, Object value) {
        return this.appendField(field, value, ">");
    }

    /**
     * 设置条件：大于等于
     * 
     * @param field 数据表字段名
     * @param value 条件值
     * @return this
     */
    public SqlWhere gte(String field, Object value) {
        return this.appendField(field, value, ">=");
    }

    /**
     * 设置条件：小于
     * 
     * @param field 数据表字段名
     * @param value 条件值
     * @return this
     */
    public SqlWhere lt(String field, Object value) {
        return this.appendField(field, value, "<");
    }

    /**
     * 设置条件：小于等于
     * 
     * @param field 数据表字段名
     * @param value 条件值
     * @return this
     */
    public SqlWhere lte(String field, Object value) {
        return this.appendField(field, value, "<=");
    }

    /**
     * 设置条件：是否为null
     * 
     * @param field 数据表字段名
     * @param value 条件值
     * @return this
     */
    public final SqlWhere isNull(String field, boolean value) {
        if (this.size++ > 0)
            sb.append(operation == OperationEnum.And ? " and " : " or ");
        sb.append(field);
        if (value)
            sb.append(" is null");
        else
            sb.append(" is not null");
        return this;
    }

    /**
     * 设置条件：是否为null
     * 
     * @param field 数据表字段名
     * @param value 从中取出相应的字段记录作为条件值
     * @return this
     */
    public final SqlWhere isNull(String field, DataRow value) {
        return isNull(field, value.getBoolean(field));
    }

    public final SqlWhere like(String field, String value) {
        if (Utils.isEmpty(value) || "*".equals(value))
            return this;
        if (value.length() == 1)
            return like(field, value, LinkOptionEnum.Right);
        else if (value.startsWith("*") && value.endsWith("*"))
            return like(field, value.substring(1, value.length() - 1), LinkOptionEnum.All);
        else if (value.startsWith("*"))
            return like(field, value.substring(1, value.length()), LinkOptionEnum.Left);
        else if (value.endsWith("*"))
            return like(field, value.substring(0, value.length() - 1), LinkOptionEnum.Right);
        else
            return like(field, value, LinkOptionEnum.Right);
    }

    public final SqlWhere like(String field, DataRow value) {
        return like(field, value.getString(field));
    }

    public final SqlWhere like(String field, String value, LinkOptionEnum linkOption) {
        if (Utils.isEmpty(value))
            return this;
        if (this.size++ > 0)
            sb.append(operation == OperationEnum.And ? " and " : " or ");
        sb.append(field);
        sb.append(" like '");
        if (linkOption == LinkOptionEnum.All || linkOption == LinkOptionEnum.Left)
            sb.append("%%");
        sb.append(value);
        if (linkOption == LinkOptionEnum.All || linkOption == LinkOptionEnum.Right)
            sb.append("%%");
        sb.append("'");
        return this;
    }

    public final SqlWhere in(String field, Object... values) {
        if (values.length == 0)
            return this;
        if (this.size++ > 0)
            sb.append(operation == OperationEnum.And ? " and " : " or ");
        sb.append(field).append(" in (");
        for (int i = 0; i < values.length; i++) {
            appendValue(values[i]);
            if (i < values.length - 1)
                sb.append(",");
        }
        sb.delete(sb.length(), sb.length());
        sb.append(")");
        return this;
    }

    public final SqlWhere between(String field, Object value1, Object value2) {
        if (value1.getClass() != value2.getClass())
            throw new RuntimeException("参数错误");
        if (this.size++ > 0)
            sb.append(operation == OperationEnum.And ? " and " : " or ");
        sb.append(field).append(" between ");
        appendValue(value1);
        sb.append(" and ");
        appendValue(value2);
        return this;
    }

    public final SqlWhere between(String field, DataRow value) {
        return between(field, value.getValue(field + "_from"), value.getValue(field + "_to"));
    }

    public SqlText build() {
        if (this.sqlText != null && sb.length() > 0)
            this.sqlText.add("where " + this.text());
        return this.sqlText;
    }

    public String text() {
        return sb.toString();
    }

    public SqlWhere clear() {
        sb.setLength(0);
        size = 0;
        return this;
    }

    public OperationEnum getOperation() {
        return operation;
    }

    public void setOperation(OperationEnum operation) {
        this.operation = operation;
    }

    public int size() {
        return size;
    }

    private SqlWhere appendField(String field, Object value, String opera) {
        if (value == null)
            return this;
        Object tmp = value;
        if (value instanceof DataRow)
            tmp = ((DataRow) value).getValue(field);
        if (tmp instanceof String && ((String) tmp).length() == 0)
            return this;
        if (this.size++ > 0)
            sb.append(operation == OperationEnum.And ? " and " : " or ");
        sb.append(field).append(opera);
        appendValue(tmp);
        return this;
    }

    private void appendValue(Object value) {
        Objects.nonNull(value);
        if (value instanceof String && ((String) value).length() == 0)
            throw new RuntimeException("not support empty string");
        if (value instanceof String) {
            String tmp = (String) value;
            if (tmp.length() > 0) {
                sb.append("'");
                sb.append(Utils.safeString((String) value));
                sb.append("'");
            }
        } else if (value instanceof Datetime) {
            sb.append("'");
            sb.append(value.toString());
            sb.append("'");
        } else if (value instanceof Integer || value instanceof Long || value instanceof Float
                || value instanceof Double)
            sb.append(value);
        else if (value instanceof Boolean)
            sb.append((Boolean) value ? 1 : 0);
        else
            throw new RuntimeException(String.format("value type not support: %s", value.getClass().getName()));
    }

}
