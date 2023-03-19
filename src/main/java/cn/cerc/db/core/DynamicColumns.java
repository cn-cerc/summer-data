package cn.cerc.db.core;

import java.util.ArrayList;
import java.util.List;

/**
 * 动态列工具，用于处理横向数据表
 * 
 * @author ZhangGong
 *
 */
public class DynamicColumns {
    private DataSet dataSet;
    private String keyField;
    private List<FieldMeta> columns = new ArrayList<>();

    public DynamicColumns(DataSet dataSet, String keyField) {
        this.dataSet = dataSet;
        this.keyField = keyField;
    }

    public FieldMeta addColumn(String columnKey, String columnName) {
        FieldMeta column = dataSet.fields().add("F" + (columns.size() + 1)).setName(columnName).setRemark(columnKey);
        columns.add(column);
        return column;
    }

    public FieldMeta findColumn(String columnKey) {
        for (FieldMeta meta : dataSet.fields()) {
            if (columnKey.equals(meta.remark())) {
                return meta;
            }
        }
        return null;
    }

    public DataCell getCell(String rowKey, String columnKey) {
        FieldMeta column = findColumn(columnKey);
        if (column == null)
            return null;

        if (!dataSet.locate(keyField, rowKey))
            dataSet.append().setValue(keyField, rowKey);
        return dataSet.currentRow().get().bind(column.code());
    }

    public List<FieldMeta> columns() {
        return columns;
    }

    public static void main(String[] args) {
        // 主数据表
        DataSet ds = new DataSet();
        ds.append().setValue("code", "a1").setValue("name", "张三");
        ds.append().setValue("code", "a2").setValue("name", "李四");
        ds.mergeChangeLog();

        // 数据来源表范例：工号为a1(张三)的，各个月份之值
        DataSet src1 = new DataSet();
        src1.append().setValue("pcode", "a1").setValue("mount", "202208").setValue("amount", 480);
        src1.append().setValue("pcode", "a2").setValue("mount", "202209").setValue("amount", 490);
        src1.append().setValue("pcode", "a2").setValue("mount", "202210").setValue("amount", 500);
        src1.append().setValue("pcode", "a4").setValue("mount", "202210").setValue("amount", 500);

        DynamicColumns sheet = new DynamicColumns(ds, "code");
        // 定义横向表的列值、标题
        sheet.addColumn("202208", "8月份");
        sheet.addColumn("202209", "9月份");
        sheet.addColumn("202210", "10月份");

        // 根据数据表给相应的列赋值
        for (DataRow row : src1) {
            DataCell cell = sheet.getCell(row.getString("pcode"), row.getString("mount"));
            if (cell != null) {
                cell.setValue(row.getString("amount"));
                if (cell.source().state() == DataRowState.Insert)
                    cell.source().setValue("name", "未知");
            }
        }

        // 手动赋值
        DataCell cell = sheet.getCell("a5", "202208");
        if (cell != null) {
            cell.setValue(1000);
            if (cell.source().state() == DataRowState.Insert)
                cell.source().setValue("name", "这是新的用户");
        }

        // 检查自定义列
        for (FieldMeta meta : sheet.columns())
            System.out.println(meta.code() + ":" + meta.name() + ":" + meta.remark());

        // 检查合并结果
        System.out.println(ds);
    }
}
