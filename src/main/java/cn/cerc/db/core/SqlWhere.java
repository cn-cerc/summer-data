package cn.cerc.db.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class SqlWhere {
    private final JoinDirectionEnum joinGroup;
    private JoinDirectionEnum joinMode = JoinDirectionEnum.And;
    private StringBuffer sb = new StringBuffer();
    private SqlText sqlText;
    // 统计加入了多少个条件
    private int size;
    // 链式调用
    private SqlWhere origin = null;
    private SqlWhere next = null;
    // 简化调用
    private DataRow dataRow = null;

    public enum JoinDirectionEnum {
        And,
        Or
    }

    public enum LinkOptionEnum {
        Right,
        Left,
        All
    }

    public SqlWhere() {
        this(JoinDirectionEnum.And);
    }

    public SqlWhere(JoinDirectionEnum joinGroup) {
        super();
        this.joinGroup = joinGroup;
    }

    /**
     * 设置分组条件为and
     * 
     * @return this
     */
    public final SqlWhere AND() {
        SqlWhere result = new SqlWhere(JoinDirectionEnum.And);
        result.origin = this.origin != null ? this.origin : this;
        this.next = result;
        return result;
    }

    /**
     * 设置分组条件为or
     * 
     * @return this
     */
    public final SqlWhere OR() {
        SqlWhere result = new SqlWhere(JoinDirectionEnum.Or);
        result.origin = this.origin != null ? this.origin : this;
        this.next = result;
        return result;
    }

    // 更改内部默认组合条件
    public final SqlWhere and() {
        this.joinMode = JoinDirectionEnum.And;
        return this;
    }

    // 更改内部默认组合条件
    public final SqlWhere or() {
        this.joinMode = JoinDirectionEnum.Or;
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
        if (value != null)
            return this.appendField(field, value, "=");
        else
            return this.isNull(field, true);
    }

    /**
     * 设置条件：相等，条件值从params中取
     * 
     * @param field 数据表字段名
     * @return this
     */
    public SqlWhere eq(String field) {
        Objects.requireNonNull(dataRow);
        String dbField = field;
        if (field.contains("."))
            field = field.split("\\.")[1];
        if (dataRow.hasValue(field))
            return this.eq(dbField, dataRow.getValue(field));
        else
            return this;
    }

    /**
     * 设置条件：不相等
     * 
     * @param field 数据表字段名
     * @param value 条件值
     * @return this
     */
    public SqlWhere neq(String field, Object value) {
        if (value != null)
            return this.appendField(field, value, "<>");
        else
            return this.isNull(field, false);
    }

    /**
     * 设置条件：不相等，条件值从params中取
     * 
     * @param field 数据表字段名
     * @return this
     */
    public SqlWhere neq(String field) {
        Objects.requireNonNull(dataRow);
        String dbField = field;
        if (field.contains("."))
            field = field.split("\\.")[1];
        return this.neq(dbField, dataRow.getValue(field));
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
     * 设置条件：大于，条件值从params中取
     * 
     * @param field 数据表字段名
     * @return this
     */
    public SqlWhere gt(String field) {
        Objects.requireNonNull(dataRow);
        String dbField = field;
        if (field.contains("."))
            field = field.split("\\.")[1];
        return this.gt(dbField, dataRow.getValue(field));
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
     * 设置条件：大于等于，条件值从params中取
     * 
     * @param field 数据表字段名
     * @return this
     */
    public SqlWhere gte(String field) {
        Objects.requireNonNull(dataRow);
        String dbField = field;
        if (field.contains("."))
            field = field.split("\\.")[1];
        return this.gte(dbField, dataRow.getValue(field));
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
     * 设置条件：小于，条件值从params中取
     * 
     * @param field 数据表字段名
     * @return this
     */
    public SqlWhere lt(String field) {
        Objects.requireNonNull(dataRow);
        String dbField = field;
        if (field.contains("."))
            field = field.split("\\.")[1];
        return this.lt(dbField, dataRow.getValue(field));
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
     * 设置条件：小于等于，条件值从params中取
     * 
     * @param field 数据表字段名
     * @return this
     */
    public SqlWhere lte(String field) {
        Objects.requireNonNull(dataRow);
        String dbField = field;
        if (field.contains("."))
            field = field.split("\\.")[1];
        return this.lte(dbField, dataRow.getValue(field));
    }

    /**
     * 设置条件：是否为null
     * 
     * @param field 数据表字段名
     * @param value 条件值
     * @return this
     */
    public final SqlWhere isNull(String field, boolean value) {
        if (field.contains("'"))
            throw new RuntimeException("field contains error character[']");
        if (this.size++ > 0)
            sb.append(joinMode == JoinDirectionEnum.And ? " and " : " or ");
        sb.append(field);
        if (value)
            sb.append(" is null");
        else
            sb.append(" is not null");
        return this;
    }

    /**
     * 设置条件：是否为null，条件值从params中取
     * 
     * @param field 数据表字段名
     * @return this
     */
    public SqlWhere isNull(String field) {
        Objects.requireNonNull(dataRow);
        String dbField = field;
        if (field.contains("."))
            field = field.split("\\.")[1];
        return this.isNull(dbField, dataRow.getBoolean(field));
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

    public final SqlWhere like(String field) {
        Objects.requireNonNull(dataRow);
        String dbField = field;
        if (field.contains("."))
            field = field.split("\\.")[1];
        return like(dbField, dataRow.getString(field));
    }

    public final SqlWhere like(String field, String value, LinkOptionEnum linkOption) {
        if (field.contains("'"))
            throw new RuntimeException("field contains error character[']");
        if (Utils.isEmpty(value))
            return this;
        if (this.size++ > 0)
            sb.append(joinMode == JoinDirectionEnum.And ? " and " : " or ");
        sb.append(field);
        sb.append(" like '");
        if (linkOption == LinkOptionEnum.All || linkOption == LinkOptionEnum.Left)
            sb.append("%");
        sb.append(value);
        if (linkOption == LinkOptionEnum.All || linkOption == LinkOptionEnum.Right)
            sb.append("%");
        sb.append("'");
        return this;
    }

    public final SqlWhere in(String field, Collection<?> values) {
        if (values == null || values.size() == 0)
            throw new RuntimeException("sql command IN conditions can not be empty");
        if (field.contains("'"))
            throw new RuntimeException("field contains error character[']");
        if (this.size++ > 0)
            sb.append(joinMode == JoinDirectionEnum.And ? " and " : " or ");
        sb.append(field).append(" in (");
        int i = 0;
        for (Object obj : values) {
            appendValue(obj);
            if (i++ < values.size() - 1)
                sb.append(",");
        }
        sb.append(")");
        return this;
    }

    /**
     * 
     * @param fields     要比较的字段列表
     * @param valuesList 要比较的字段值
     * @return 比对多个字段是否一致，形如：(f1=v1 and f2=v2) or (f1=v1 and f2=v2)
     */
    public final SqlWhere inGroup(List<String> fields, List<Object[]> valuesList) {
        if (fields.size() == 0)
            throw new IllegalArgumentException("fields is empty array");
        if (valuesList.size() == 0)
            return this;
        if (this.size++ > 0)
            sb.append(joinMode == JoinDirectionEnum.And ? " and " : " or ");

        sb.append("(");
        for (int row = 0; row < valuesList.size(); row++) {
            Object[] values = valuesList.get(row);
            if (values.length != fields.size())
                throw new IllegalArgumentException("fields.length neq values.length");
            sb.append("(");
            for (int i = 0; i < values.length; i++) {
                sb.append(fields.get(i));
                sb.append("=");
                appendValue(values[i]);
                if (i < values.length - 1)
                    sb.append(" and ");
            }
            sb.append(")");
            if (row < valuesList.size() - 1)
                sb.append(" or ");
        }
        sb.append(")");

        return this;
    }

    public final SqlWhere in(String field, String sqlText) {
        if (field.contains("'"))
            throw new RuntimeException("field contains error character[']");
        if (Utils.isEmpty(sqlText))
            return this;
        if (this.size++ > 0)
            sb.append(joinMode == JoinDirectionEnum.And ? " and " : " or ");
        sb.append(field).append(" in (");
        sb.append(sqlText);
        sb.append(")");
        return this;
    }

    public final SqlWhere between(String field, Object value1, Object value2) {
        if (field.contains("'"))
            throw new RuntimeException("field contains error character[']");
        if (value1.getClass() != value2.getClass())
            throw new RuntimeException("参数错误");
        if (this.size++ > 0)
            sb.append(joinMode == JoinDirectionEnum.And ? " and " : " or ");
        sb.append(field).append(" between ");
        appendValue(value1);
        sb.append(" and ");
        appendValue(value2);
        return this;
    }

    public final SqlWhere between(String field) {
        Objects.requireNonNull(dataRow);
        String dbField = field;
        if (field.contains("."))
            field = field.split("\\.")[1];
        return between(dbField, dataRow.getValue(field + "_from"), dataRow.getValue(field + "_to"));
    }

    public SqlText build() {
        SqlWhere origin = this.origin != null ? this.origin : this;
        if (origin.sqlText != null && origin.sb.length() > 0)
            origin.sqlText.add("where " + origin.toString());
        return origin.sqlText;
    }

    public SqlText build(SqlText sqlText) {
        Objects.requireNonNull(sqlText);
        SqlWhere origin = this.origin != null ? this.origin : this;
        origin.sqlText = sqlText;
        return this.build();
    }

    @Override
    public String toString() {
        if (this.origin != null)
            return this.origin.toString();
        if (this.next == null)
            return sb.toString();
        List<SqlWhere> list = new ArrayList<>();
        SqlWhere item = this;
        do {
            if (item.size > 0)
                list.add(item);
            item = item.next;
        } while (item != null);
        if (list.size() == 0)
            return "";
        StringBuffer result = new StringBuffer();
        for (SqlWhere where : list) {
            if (result.length() > 0)
                result.append(where.joinGroup == JoinDirectionEnum.And ? " AND " : " OR ");
            result.append("(").append(where.text()).append(")");
        }
        return result.toString();
    }

    public String text() {
        return sb.toString();
    }

    public SqlWhere clear() {
        sb.setLength(0);
        size = 0;
        return this;
    }

    public int size() {
        return size;
    }

    public JoinDirectionEnum getJoinMode() {
        return joinMode;
    }

    public void setJoinMode(JoinDirectionEnum operation) {
        this.joinMode = operation;
    }

    private SqlWhere appendField(String field, Object value, String opera) {
        if (field.contains("'"))
            throw new RuntimeException("field contains error character[']");
        if (value == null)
            return this;
        if (this.size++ > 0)
            sb.append(joinMode == JoinDirectionEnum.And ? " and " : " or ");
        sb.append(field).append(opera);
        appendValue(value);
        return this;
    }

    private void appendValue(Object value) {
        Objects.requireNonNull(value);
        if (value instanceof String) {
            String tmp = (String) value;
            sb.append("'");
            if (tmp.length() > 0)
                sb.append(Utils.safeString((String) value));
            sb.append("'");
        } else if (value instanceof Datetime) {
            sb.append("'");
            sb.append(value.toString());
            sb.append("'");
        } else if (value instanceof Integer || value instanceof Long || value instanceof Float
                || value instanceof Double)
            sb.append(value);
        else if (value instanceof Boolean)
            sb.append((Boolean) value ? 1 : 0);
        else if (value instanceof Enum)
            sb.append(((Enum<?>) value).ordinal());
        else
            throw new RuntimeException(String.format("value type not support: %s", value.getClass().getName()));
    }

    public SqlText sqlText() {
        return sqlText;
    }

    public SqlWhere setSqlText(SqlText sqlText) {
        this.sqlText = sqlText;
        return this;
    }

    public DataRow dataRow() {
        return dataRow;
    }

    public SqlWhere setDataRow(DataRow dataRow) {
        this.dataRow = dataRow;
        return this;
    }

    public JoinDirectionEnum getJoinGroup() {
        return joinGroup;
    }

    public static SqlWhere create(Class<? extends EntityImpl> clazz) {
        return new SqlText(clazz).addSelectDefault().addWhere();
    }

    public static SqlWhere create(IHandle handle, Class<? extends EntityImpl> clazz, String... values) {
        EntityKey entityKey = clazz.getDeclaredAnnotation(EntityKey.class);
        int offset = entityKey.corpNo() ? 1 : 0;
        SqlWhere where = SqlWhere.create(clazz);
        if (entityKey.corpNo())
            where.eq(entityKey.fields()[0], handle.getCorpNo());
        for (int i = 0; i < values.length; i++) {
            String field = entityKey.fields()[i + offset];
            Object value = values[i];
            if (value == null)
                where.isNull(field);
            else
                where.eq(field, value);
        }
        return where;
    }

    public static void main(String[] args) {
        // code is null
        System.out.println(new SqlWhere().eq("code", null));

        // code in (1,3,4)
        System.out.println(new SqlWhere().in("code", List.of(1, 3, 4)));

        // code in (select code_ from xxx)
        System.out.println(new SqlWhere().in("code", "select code_ from xxx"));

        // ((WHCode='A01' and PartCode='P01') or (WHCode='A01' and PartCode='P02'))
        List<Object[]> items = new ArrayList<>();
        items.add(new Object[] { "A01", "P01" });
        items.add(new Object[] { "A01", "P02" });
        System.out.println(new SqlWhere().inGroup(List.of("WHCode", "PartCode"), items));
    }

}
