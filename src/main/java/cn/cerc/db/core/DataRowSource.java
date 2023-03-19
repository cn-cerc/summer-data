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
        var dataRow = currentRow().orElse(null);
        if (dataRow == null)
            return true;
        return dataRow.readonly();
    }

}
