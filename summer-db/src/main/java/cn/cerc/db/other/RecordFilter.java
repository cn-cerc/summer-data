package cn.cerc.db.other;

import java.util.ArrayList;
import java.util.List;

import cn.cerc.core.DataRow;
import cn.cerc.core.DataSet;
import cn.cerc.core.FieldMeta;
import cn.cerc.core.Utils;

/**
 * 对数据返回的记录进行过滤，以降低网络流量
 * 
 * @author ZhangGong
 *
 */
public class RecordFilter {

    public static DataSet execute(DataSet dataIn, DataSet dataOut) {
        String sql = dataIn.getHead().getString("_RecordFilter_");
        if (Utils.isEmpty(sql) || dataOut.state() < 1)
            return dataOut;
        return execute(dataOut, sql);
    }

    private static DataSet execute(DataSet dataOut, String sql) {
        SqlTextDecode processor = new SqlTextDecode(sql);
        // 防止数据回写到资料库
        dataOut.disableStorage();
        // 删减字段
        if (processor.getFieldDefs() != null) {
            List<FieldMeta> items = new ArrayList<>();
            for (FieldMeta meta : dataOut.getFieldDefs()) 
                items.add(meta);
            for (FieldMeta meta : items) {
                if (!processor.getFieldDefs().exists(meta.getCode()))
                    dataOut.getFieldDefs().remove(meta.getCode());
            }
        }
        // 删减记录
        if (processor.getWhere().size() > 0) {
            dataOut.first();
            while (dataOut.fetch()) {
                DataRow src = dataOut.current();
                if (processor.filter(src))
                    dataOut.next();
                else
                    dataOut.delete();
            }
        }
        return dataOut;
    }

    public static void main(String[] args) {
        DataSet ds1 = new DataSet();
        ds1.append().setValue("code", "a01").setValue("name", "jason");
        ds1.append().setValue("code", "a02").setValue("name", "jason");
        ds1.getFieldDefs().get("code").setName("代码");
        ds1.setMetaInfo(true);

        System.out.println(RecordFilter.execute(ds1, "select code from xxx where code=a01"));
        System.out.println(RecordFilter.execute(ds1, "select code,name from xxx"));
    }
}
