package cn.cerc.db.dao;

public interface EntityEvent {
    // 在保存之前可执行的代码
    default void beforePost() {

    }

    // 在保存之后可执行的代码
    default void afterPost() {

    }
}
