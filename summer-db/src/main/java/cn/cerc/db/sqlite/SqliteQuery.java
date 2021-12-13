package cn.cerc.db.sqlite;

import cn.cerc.core.DataSetGson;
import cn.cerc.core.SqlServerType;
import cn.cerc.core.Utils;
import cn.cerc.db.core.SqlQuery;

public class SqliteQuery extends SqlQuery {
    private static final long serialVersionUID = 927151029588126209L;

    public SqliteQuery() {
        super(null, SqlServerType.Sqlite);
    }

    @Override
    public String json() {
        return new DataSetGson<>(this).encode();
    }

    @Override
    public SqliteQuery setJson(String json) {
        this.clear();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        return this;
    }

}
