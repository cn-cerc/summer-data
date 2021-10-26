package cn.cerc.db.core;

public interface DataSource {

    DataRow getCurrent();

    boolean isReadonly();
}
