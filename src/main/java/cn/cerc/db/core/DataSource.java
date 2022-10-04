package cn.cerc.db.core;

public interface DataSource {

    /**
     * @return 返回 dataSet，可为null
     */
    default DataSet dataSet() {
        return null;
    }

    default DataRow dataRow() {
        return null;
    }

    /**
     * 
     * @return 返回dataRow，若dataSet()不为空，则为dataSet.current()
     */
    default DataRow current() {
        var dataSet = dataSet();
        return dataSet != null ? dataSet.current() : dataRow();
    }

    /**
     * 
     * @return 返回数据是否只读，默认值为 true
     */
    default boolean readonly() {
        var current = current();
        return current != null ? current().readonly() : true;
    }

//    @Deprecated
//    default DataRow getCurrent() {
//        return current();
//    }

//    @Deprecated
//    default boolean isReadonly() {
//        return readonly();
//    }

}
