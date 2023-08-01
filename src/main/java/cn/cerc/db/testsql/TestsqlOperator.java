package cn.cerc.db.testsql;

import java.sql.Connection;
import java.sql.SQLException;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlOperator;
import cn.cerc.db.core.SqlServerType;

public class TestsqlOperator extends SqlOperator {

    public TestsqlOperator(IHandle handle, SqlServerType sqlServerType) {
        super(handle, sqlServerType);
    }

    @Override
    public int select(DataSet dataSet, Connection connection, String sql) throws SQLException {
        TestsqlServer.get().select(dataSet, this.table(), sql);
        return dataSet.size();
    }

    @Override
    public boolean insert(Connection connection, DataRow dataRow) {
        if (TestsqlServer.DefaultOID.equalsIgnoreCase(this.oid())) {
            if ("".equals(dataRow.getString(this.oid()))) {
                var dataSet = dataRow.dataSet();
                var uid = 0l;
                for (var row : dataSet) {
                    if (row.getLong(TestsqlServer.DefaultOID) > uid)
                        uid = row.getLong(TestsqlServer.DefaultOID);
                }
                dataRow.setValue(this.oid(), uid + 1);
            }
        }
        return true;
    }

    @Override
    public boolean delete(Connection connection, DataRow record) {
        return true;
    }

    @Override
    public boolean update(Connection connection, DataRow record) {
        return true;
    }

}
