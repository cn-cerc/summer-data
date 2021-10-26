package cn.cerc.db.mysql;

import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlQuery;
import cn.cerc.db.core.Utils;

public class MysqlQuery extends SqlQuery implements IHandle {
    private static final long serialVersionUID = -400986212909017761L;
    private MysqlServer server;
    private MysqlServer master;
    private MysqlServer salve;

    public MysqlQuery() {
        super();
    }

    public MysqlQuery(IHandle handle) {
        super(handle);
    }

    @Override
    public final MysqlServer getServer() {
        if (server != null)
            return server;

        if (master == null)
            master = (MysqlServer) getSession().getProperty(MysqlServerMaster.SessionId);
        if (this.isStorage()) {
            return master;
        } else {
            if (salve == null) {
                salve = (MysqlServer) getSession().getProperty(MysqlServerSlave.SessionId);
                if (salve == null)
                    salve = master;
                if (salve.getHost().equals(master.getHost()))
                    salve = master;
            }
            return salve;
        }
    }

    public void setServer(MysqlServer server) {
        if (this.isActive())
            throw new RuntimeException("server change fail on active");
        this.server = server;
    }

    @Override
    public String toJson() {
        return new DataSetGson<>(this).encode();
    }

    @Override
    public MysqlQuery fromJson(String json) {
        this.close();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        return this;
    }
    
}
