package cn.cerc.db.mssql;

import cn.cerc.core.ISession;
import cn.cerc.db.mysql.QueryHelper;

public class MssqlQueryHelper extends QueryHelper<MssqlQuery> {

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

}
