package cn.cerc.db.elasticsearch.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import cn.cerc.db.elasticsearch.enums.FieldType;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.ANNOTATION_TYPE })
public @interface Field {

    String name() default "";

    FieldType type() default FieldType.Keyword;

    String analyzer() default "";

    String format() default "";// yyyy-MM-dd HH:mm:ss
}
