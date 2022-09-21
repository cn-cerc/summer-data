package cn.cerc.db.core;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Target(TYPE)
@Retention(RUNTIME)
public @interface AutoLog {
    //TODO 这里用不了HistoryType 暂时用int 测试
    int historyType() default 0;
}
