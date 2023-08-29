package cn.cerc.db.core;

import java.util.ArrayList;
import java.util.List;

public class DataSheet {
    private List<DataSet> items = new ArrayList<>();
    private List<Column> columns = new ArrayList<>();
    private DataSet dataOut;

    private void addSource(DataSet dataSet, String fieldCode, int fieldNo) {
        if (items.indexOf(dataSet) == -1)
            items.add(dataSet);
    }

    public record Column(String code, String define) {
    }

    private void addColumn(String fieldCode, String define) {
        this.columns.add(new Column(fieldCode, define));
    }

    private DataSet build() {
        dataOut = new DataSet();
        return dataOut;
    }

    public static void main(String[] args) {
        DataSet ds1 = new DataSet();
        ds1.append().setValue("code", "a1").setValue("name", "jason1");
        ds1.append().setValue("code", "a2").setValue("name", "jason2");
        ds1.append().setValue("code", "a3").setValue("name", "jason3");

        DataSet ds2 = new DataSet();
        ds2.append().setValue("code", "a1").setValue("num", 100);
        ds2.append().setValue("code", "a3").setValue("num", 300);

        DataSheet sheet = new DataSheet();
        sheet.addSource(ds1, "code", 100);
        sheet.addSource(ds1, "name", 101);

        sheet.addSource(ds2, "code", 200);
        sheet.addSource(ds2, "num", 201);

        sheet.addColumn("A1", "=me(100)");
        sheet.addColumn("A2", "=me(101)");
        sheet.addColumn("A3", "=A1()+A2()");
        sheet.addColumn("A4", "=locate(200, me(100), me(201))");

        sheet.build();
    }
}
