package cn.cerc.db.other;

import cn.cerc.db.core.DataRow;

public class SqlFieldFilter {
    // 关系：and or
    private FieldWhereRelation relation;
    // 字段
    private String field;
    // 比较方式
    private String operation;
    // 比较值
    private String value;

    public enum FieldWhereRelation {
        And, Or;
    }

    public SqlFieldFilter(FieldWhereRelation relation, String text) {
        this.relation = relation;

        // code_="a01"
        this.operation = "=";
        String[] map = text.split(this.operation);
        if (map.length != 2)
            throw new RuntimeException(String.format("not support where: %s", text));

        this.field = map[0].trim();
        this.value = map[1].trim();
        if (value.startsWith("'") && value.endsWith("'"))
            this.value = value.substring(1, value.length() - 1);
    }

    public boolean pass(DataRow row) {
        if (this.relation != FieldWhereRelation.And)
            throw new RuntimeException("not support this relation: " + this.relation);
        if (!"=".equals(this.operation))
            throw new RuntimeException("not support this operation: " + this.operation);
        return this.value.equals(row.getString(this.field));
    }

    public static void main(String[] args) {
        DataRow row = new DataRow();
        row.setValue("code", "");
        SqlFieldFilter fw = new SqlFieldFilter(FieldWhereRelation.And, "code=''");
        System.out.println(fw.pass(row));
        row.setValue("code", "value2");
        System.out.println(fw.pass(row));
    }

    public final String getField() {
        return field;
    }

    public final String getOperation() {
        return operation;
    }

    public final String getValue() {
        return value;
    }
}
