package cn.cerc.db.core;

public interface DataSource {

    DataRow current();

    @Deprecated
    default DataRow getCurrent() {
        return current();
    }

    boolean isReadonly();
}
