package cn.cerc.core;

public @interface Describe {

    String name();

    String remark() default "";

    int version() default 0;
}
