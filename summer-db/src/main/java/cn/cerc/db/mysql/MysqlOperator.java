package cn.cerc.db.mysql;

import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlOperator;
import cn.cerc.db.core.SqlServerType;

@Deprecated
public class MysqlOperator extends SqlOperator {

    public MysqlOperator(IHandle handle) {
        super(handle, SqlServerType.Mysql);
    }

}
