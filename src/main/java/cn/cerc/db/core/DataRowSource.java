package cn.cerc.db.core;

import java.util.Optional;

public interface DataRowSource extends DataSource {

    /**
     * 
     * @return 返回当前记录 dataRow
     */
    Optional<DataRow> currentRow();

    /**
     * 
     * @return 返回数据是否只读，默认值为 true
     */
    @Override
    default boolean readonly() {
        return currentRow().map(row -> row.readonly()).orElse(true);
    }

}
