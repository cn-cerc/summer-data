package cn.cerc.db.mssql;

import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlQuery;
import cn.cerc.db.core.SqlText;
import cn.cerc.db.core.Utils;

public class MssqlQuery extends SqlQuery implements IHandle {
    private static final long serialVersionUID = -3510548502879617750L;
    private MssqlServer server = null;

    public MssqlQuery() {
        super();
    }

    public MssqlQuery(IHandle handle) {
        super(handle);
        this.getSqlText().setServerType(SqlText.SERVERTYPE_MSSQL);
    }

    @Override
    public MssqlServer getServer() {
        if (server == null)
            server = (MssqlServer) getSession().getProperty(MssqlServer.SessionId);
        return server;
    }

    public void setServer(MssqlServer server) {
        this.server = server;
    }

    @Override
    public String toJson() {
        return new DataSetGson<>(this).encode();
    }

    @Override
    public MssqlQuery fromJson(String json) {
        this.close();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        return this;
    }
}
