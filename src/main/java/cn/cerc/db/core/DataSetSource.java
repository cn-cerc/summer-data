package cn.cerc.db.core;

import java.util.Optional;

public interface DataSetSource extends DataSource {

    /**
     * 
     * @return 返回当前数据集
     */
    Optional<DataSet> getDataSet();

    /**
     * 
     * @return 返回数据是否只读，默认值为 true
     */
    @Override
    default boolean readonly() {
        var dataSet = getDataSet().orElse(null);
        if (dataSet == null)
            return true;
        return dataSet.getDataRow().map(item -> item.readonly()).orElse(true);
    }

}
