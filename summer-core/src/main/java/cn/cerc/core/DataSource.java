package cn.cerc.core;

public interface DataSource {

    DataRow getCurrent();

    boolean isReadonly();
}
