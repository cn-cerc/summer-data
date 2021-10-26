package cn.cerc.db.sqlite;

import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.SqlQuery;
import cn.cerc.db.core.SqlText;
import cn.cerc.db.core.Utils;

public class SqliteQuery extends SqlQuery {
    private static final long serialVersionUID = 927151029588126209L;
    private SqliteServer server = null;

    public SqliteQuery() {
        super();
        this.getSqlText().setServerType(SqlText.SERVERTYPE_SQLITE);
    }

    @Override
    public SqliteServer getServer() {
        if (server == null)
            server = new SqliteServer();
        return server;
    }

    public void setServer(SqliteServer server) {
        this.server = server;
    }

    @Override
    public String toJson() {
        return new DataSetGson<>(this).encode();
    }

    @Override
    public SqliteQuery fromJson(String json) {
        this.close();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        return this;
    }

}
