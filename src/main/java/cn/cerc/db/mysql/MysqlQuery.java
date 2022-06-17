package cn.cerc.db.mysql;

import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlQuery;
import cn.cerc.db.core.SqlServerType;
import cn.cerc.db.core.Utils;

public class MysqlQuery extends SqlQuery implements IHandle {
    private static final long serialVersionUID = -400986212909017761L;

    public MysqlQuery() {
        this(null);
    }

    public MysqlQuery(IHandle handle) {
        super(handle, SqlServerType.Mysql);
    }

    @Override
    public String json() {
        return new DataSetGson<>(this).encode();
    }

    @Override
    public MysqlQuery setJson(String json) {
        this.clear();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        return this;
    }

}
