package cn.cerc.db.mysql;

import cn.cerc.core.ISession;

public class MysqlQueryHelper extends QueryHelper<MysqlQuery> {

    public MysqlQueryHelper(ISession session) {
        super(session);
    }
    
    public MysqlQueryHelper(MysqlQuery query) {
        super(query);
    }

    @Override
    public MysqlQuery dataSet() {
        if (this.dataSet == null)
            this.dataSet = new MysqlQuery(this);
        return this.dataSet;
    }
    
    @Override
    public String sqlText() {
        String text = super.sqlText();
        if ("".equals(text)) 
            return text;
        if (dataSet().sql().maximum() > -1) {
            return text + vbCrLf + "limit " + dataSet().sql().maximum();
        } else {
            return text;
        }
    }

}
