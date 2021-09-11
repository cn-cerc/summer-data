package cn.cerc.core;

public interface DataSource {

    Record getCurrent();

    boolean isReadonly();
}
