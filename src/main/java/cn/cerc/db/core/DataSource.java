package cn.cerc.db.core;

public interface DataSource {
    
    default DataSet dataSet() {
        return null;
    }

    default DataRow current() {
        var dataSet = dataSet();
        return dataSet != null ? dataSet.current() : null;
    }

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
