package cn.cerc.db.testsql;

import cn.cerc.db.core.DataSetGson;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlQuery;
import cn.cerc.db.core.SqlServerType;
import cn.cerc.db.core.Utils;

public class TestsqlQuery extends SqlQuery implements IHandle {
    private static final long serialVersionUID = 4090176199581107313L;

    public TestsqlQuery() {
        this(null);
    }

    public TestsqlQuery(IHandle handle) {
        super(handle, SqlServerType.Testsql);
    }

    @Override
    public String json() {
        return new DataSetGson<>(this).encode();
    }

    @Override
    public TestsqlQuery setJson(String json) {
        this.clear();
        if (!Utils.isEmpty(json))
            new DataSetGson<>(this).decode(json);
        return this;
    }

}
