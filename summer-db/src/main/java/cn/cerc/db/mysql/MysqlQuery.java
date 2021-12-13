package cn.cerc.db.mysql;

import cn.cerc.core.DataSetGson;
import cn.cerc.core.SqlServerType;
import cn.cerc.core.Utils;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlQuery;

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
