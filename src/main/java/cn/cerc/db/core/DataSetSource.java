package cn.cerc.db.core;

import java.util.Optional;

public interface DataSetSource {

    Optional<DataSet> source();

//    /**
//     * 请改使用语义更清晰的 source 函数
//     * 
//     * @return 返回 dataSet，可为null
//     */
//    @Deprecated
//    default DataSet dataSet() {
//        return source().orElse(null);
//    }

    /**
     * 
     * @return 返回当前记录 dataRow
     */
    default Optional<DataRow> currentRow() {
        return source().map(dataSet -> dataSet.currentRow()).orElse(Optional.empty());
    }

//    /**
//     * 请改为语义更清晰的currentRow函数
//     * 
//     * @return 返回dataRow，若dataSet()不为空，则为dataSet.current()
//     */
//    @Deprecated
//    default DataRow current() {
//        return currentRow().orElse(null);
//    }

    /**
     * 
     * @return 返回数据是否只读，默认值为 true
     */
    default boolean readonly() {
        return currentRow().map(item -> item.readonly()).orElse(true);
    }

}
