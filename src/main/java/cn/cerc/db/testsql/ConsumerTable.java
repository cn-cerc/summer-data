package cn.cerc.db.testsql;

import cn.cerc.db.core.DataSet;

public interface ConsumerTable {

    void accept(DataSet dataSet, String sql);

}
