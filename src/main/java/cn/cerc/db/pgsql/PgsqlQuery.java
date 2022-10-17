package cn.cerc.db.pgsql;

import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlQuery;
import cn.cerc.db.core.SqlServerType;
import cn.cerc.db.core.Utils;

public class PgsqlQuery extends SqlQuery implements IHandle {
    private static final long serialVersionUID = 3531713694200808149L;

    public PgsqlQuery() {
        this(null);
    }

    public PgsqlQuery(IHandle handle) {
        super(handle, SqlServerType.Pgsql);
    }

    @Override
    public String json() {
        return new DataSetGson<>(this).encode();
    }

    @Override
    public PgsqlQuery setJson(String json) {
        this.clear();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        return this;
    }

}
