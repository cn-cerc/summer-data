package cn.cerc.db.other;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.FieldDefs;
import cn.cerc.db.core.FieldMeta;
import cn.cerc.db.core.Utils;

public class RecordFilter {
    private DataSet dataSet;
    private SqlTextDecode processor;

    public RecordFilter(DataSet dataSet, String sql) {
        this.dataSet = dataSet;
        this.processor = new SqlTextDecode(sql);
    }

    private DataSet get() {

        // 复制head
        DataSet out = new DataSet();
        out.getHead().getFieldDefs().copy(this.dataSet.getHead().getFieldDefs());
        out.getHead().copyValues(this.dataSet.getHead());

        // 复制body.meta
        FieldDefs fieldDefs = processor.getFieldDefs();
        if (fieldDefs != null) {
            for (FieldMeta meta : this.dataSet.getFieldDefs()) {
                if (fieldDefs.exists(meta.getCode()))
                    out.getFieldDefs().add(meta.clone());
            }
        } else
            fieldDefs = this.dataSet.getFieldDefs();

        // 复制body.data
        this.dataSet.first();
        while (this.dataSet.fetch()) {
            DataRow src = this.dataSet.getCurrent();
            if (processor.filter(src)) {
                out.append();
                out.getCurrent().copyValues(src, fieldDefs);
            }
        }

        // 复制状态
        out.setState(this.dataSet.getState());
        out.setMessage(this.dataSet.getMessage());
        out.setMetaInfo(this.dataSet.isMetaInfo());
        return out;
    }

    public final SqlTextDecode getDecode() {
        return processor;
    }

    public static DataSet execute(DataSet dataIn, DataSet dataOut) {
        String sql = dataIn.getHead().getString("_RecordFilter_");
        if (Utils.isEmpty(sql) || dataOut.getState() < 1)
            return dataOut;
        return new RecordFilter(dataOut, sql).get();
    }

    public static void main(String[] args) {
        DataSet ds1 = new DataSet();
        ds1.append();
        ds1.setValue("code", "a01");
        ds1.getFieldDefs().get("code").setName("代码");
        ds1.setMetaInfo(true);
        DataSet ds2 = RecordFilter.execute(new DataSet(), ds1);
        System.out.println(ds1.toJson());
        System.out.println(ds2.toJson());
    }
}
