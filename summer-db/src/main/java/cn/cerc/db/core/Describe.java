package cn.cerc.db.core;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD })
@Retention(RUNTIME)
public @interface Describe {
    // 描述或备注
    String value() default "";

    // 名称
    String name() default "";

    // 版本
    int version() default 1;

    // 默认值
    String def() default "";

}
