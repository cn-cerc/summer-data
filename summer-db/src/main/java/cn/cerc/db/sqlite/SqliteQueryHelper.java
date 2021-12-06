package cn.cerc.db.sqlite;

import cn.cerc.core.ISession;
import cn.cerc.db.mysql.SqlQueryHelper;

public class SqliteQueryHelper extends SqlQueryHelper<SqliteQuery> {

    public SqliteQueryHelper(ISession session) {
        super(session);
    }

    public SqliteQueryHelper(SqliteQuery query) {
        super(query);
    }

    @Override
    public SqliteQuery dataSet() {
        if (this.dataSet == null)
            this.dataSet = new SqliteQuery();
        return this.dataSet;
    }

}
