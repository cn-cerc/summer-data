package cn.cerc.db.mssql;

import cn.cerc.core.ISession;
import cn.cerc.db.mysql.SqlQueryHelper;

public class MssqlQueryHelper extends SqlQueryHelper<MssqlQuery> {

    public MssqlQueryHelper(ISession session) {
        super(session);
    }

    public MssqlQueryHelper(MssqlQuery query) {
        super(query);
    }

    @Override
    public MssqlQuery dataSet() {
        if (this.dataSet == null)
            this.dataSet = new MssqlQuery(this);
        return this.dataSet;
    }

    @Override
    public String sqlText() {
        String text = super.sqlText();
        if ("".equals(text))
            return text;
        if (dataSet().sql().maximum() > -1) {
            if (text.toLowerCase().startsWith("select ")) {
                return "select top " + dataSet().sql().maximum() + " " + text.substring(7, text.length());
            } else
                return text + vbCrLf + "limit " + dataSet().sql().maximum();
        } else {
            return text;
        }
    }

    public static void main(String[] args) {
        MssqlQueryHelper query = new MssqlQueryHelper(new MssqlQuery());
        query.setSelect("select * from dept");
        query.addSelect("inner join abc on a=b");
        query.setWhere("where Code_='abc'");
        query.addWhere("Name_", "abc");
        query.setOrder("order by code_,name_ desc");
        query.setGroup("group by code_");
        System.out.println(query.sqlText());
    }
}
