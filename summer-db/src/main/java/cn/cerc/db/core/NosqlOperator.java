package cn.cerc.db.core;

import cn.cerc.core.DataRow;

public interface NosqlOperator {

    boolean insert(DataRow record);

    boolean update(DataRow record);

    boolean delete(DataRow record);

}
