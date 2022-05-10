package cn.cerc.db.other;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.FieldDefs;
import cn.cerc.db.core.SqlText;
import cn.cerc.db.other.SqlFieldFilter.FieldWhereRelation;

/**
 * 解码Sql指令
 * 
 * @author sz9214e@qq.com
 *
 */
public class SqlTextDecode {
    private static final String ORDER_FLAG = "order by";
    private FieldDefs fieldDefs;
    private List<SqlFieldFilter> whereItems = new ArrayList<>();
    Map<String, String> orderItems = new LinkedHashMap<>();;
    private String sql;

    public SqlTextDecode(String sql) {
        super();
        this.sql = sql;

        // 查找字段定义
        int offset1 = this.sql.toLowerCase().indexOf("select");
        int offset2 = this.sql.toLowerCase().indexOf("from");
        if ((offset1 > -1) && (offset2 > -1)) {
            String fields = this.sql.substring(offset1 + 6, offset2).trim();
            if (!"*".equals(fields)) {
                fieldDefs = new FieldDefs();
                for (String field : fields.split(","))
                    fieldDefs.add(field);
            }
        }

        // 查找过滤条件
        int offset3 = this.sql.toLowerCase().indexOf("where");
        if (offset3 > -1) {
            int endIndex = this.sql.toLowerCase().indexOf(ORDER_FLAG);
            String[] items;
            if (endIndex > -1) {
                items = this.sql.substring(offset3 + 5, endIndex).split(" and ");
            } else {
                items = this.sql.substring(offset3 + 5).split(" and ");
            }

            FieldWhereRelation relation = FieldWhereRelation.And;
            for (String item : items)
                whereItems.add(new SqlFieldFilter(relation, item));
        }

        // 查找排序条件
        int offset4 = this.sql.toLowerCase().indexOf(ORDER_FLAG);
        if (offset4 > -1) {
            String[] items = this.sql.substring(offset4 + ORDER_FLAG.length()).split(",");
            for (String item : items) {
                String str = item.trim();
                if (str.split(" ").length == 2) {
                    String[] tmp = str.split(" ");
                    if ("ASC".equals(tmp[1])) {
                        orderItems.put(tmp[0], "ASC");
                    } else if ("DESC".equals(tmp[1])) {
                        orderItems.put(tmp[0], "DESC");
                    } else {
                        throw new RuntimeException("暂不支持的排序条件：" + str);
                    }
                } else {
                    orderItems.put(str, "ASC");
                }
            }
        }
    }

    public String getTable() {
        return SqlText.findTableName(this.sql);
    }

    public FieldDefs fields() {
        return this.fieldDefs;
    }

    public List<SqlFieldFilter> getWhere() {
        return this.whereItems;
    }

    public Map<String, String> getOrder() {
        return this.orderItems;
    }

    public boolean filter(DataRow src) {
        for (SqlFieldFilter filter : this.whereItems) {
            if (!filter.pass(src))
                return false;
        }
        return true;
    }

    public static void main(String[] args) {
        SqlTextDecode decode = new SqlTextDecode("select code_,name_ from SvrDept where a='a1' and b='bb1' order by a,b");
        System.out.println(decode.fields());
        System.out.println(decode.getTable());
        for (SqlFieldFilter fw : decode.getWhere())
            System.out.println(new Gson().toJson(fw));
        System.out.println(decode.getOrder());
    }

}
