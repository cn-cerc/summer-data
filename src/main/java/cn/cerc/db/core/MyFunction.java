package cn.cerc.db.core;

import java.io.Serializable;

@FunctionalInterface
public interface MyFunction<T> extends Serializable {

    Object apply(T t);

}