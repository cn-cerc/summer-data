package cn.cerc.db.mysql;

import cn.cerc.core.ISession;

public class MysqlQueryHelper extends SqlQueryHelper<MysqlQuery> {

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

}
