package cn.cerc.db.sqlite;

import cn.cerc.core.DataSetGson;
import cn.cerc.core.SqlText;
import cn.cerc.core.Utils;
import cn.cerc.db.core.SqlQuery;

public class SqliteQuery extends SqlQuery {
    private static final long serialVersionUID = 927151029588126209L;
    private SqliteServer server = null;

    public SqliteQuery() {
        super();
        this.sql().setServerType(SqlText.SERVERTYPE_SQLITE);
    }

    @Override
    public SqliteServer server() {
        if (server == null)
            server = new SqliteServer();
        return server;
    }

    public void setServer(SqliteServer server) {
        this.server = server;
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
