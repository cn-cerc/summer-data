package cn.cerc.db.core;

public interface DataSource {

    DataRow current();

    @Deprecated
    default DataRow getCurrent() {
        return current();
    }

    default boolean isReadonly() {
        return current().readonly();
    }
}
