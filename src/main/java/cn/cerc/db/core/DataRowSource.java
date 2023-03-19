package cn.cerc.db.core;

import java.util.Optional;

public interface DataRowSource {

    /**
     * 
     * @return 返回当前记录 dataRow
     */
    Optional<DataRow> source();

}
