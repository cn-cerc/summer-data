package cn.cerc.core;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD })
@Retention(RUNTIME)
public @interface Describe {

    String name();

    String remark() default "";

    int version() default 0;
}
