package cn.cerc.db.other;

import cn.cerc.core.DataRow;
import cn.cerc.core.DataSet;
import cn.cerc.core.FieldDefs;
import cn.cerc.core.FieldMeta;
import cn.cerc.core.Utils;

public class RecordFilter {
    private String sql;
    private DataSet dataSet;

    public RecordFilter(String sql, DataSet dataSet) {
        this.sql = sql;
        this.dataSet = dataSet;
    }

    private DataSet get() {
        SqlTextDecode decode = new SqlTextDecode(sql);

        // 复制head
        DataSet out = new DataSet();
        out.getHead().getFieldDefs().copy(this.dataSet.getHead().getFieldDefs());
        out.getHead().copyValues(this.dataSet.getHead());

        // 复制body.meta
        FieldDefs fieldDefs = decode.getFieldDefs();
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
            if (decode.filter(src)) {
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

    public static DataSet execute(String sql, DataSet dataSet) {
        return Utils.isEmpty(sql) ? dataSet : new RecordFilter(sql, dataSet).get();
    }

}
