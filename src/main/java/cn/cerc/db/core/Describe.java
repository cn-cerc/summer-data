package cn.cerc.db.core;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD })
@Retention(RUNTIME)
public @interface Describe {

    // 字段标准
    String name();

    // 备注
    String remark() default "";

    // 版本
    int version() default 1;

    // 默认值
    String def() default "";

    // 建议显示宽度
    int width() default 0;
}
