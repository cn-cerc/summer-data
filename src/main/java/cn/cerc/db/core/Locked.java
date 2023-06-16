package cn.cerc.db.core;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * 一个表应该只有一个字段被标识为Locked字段，一般命名为lock_，可以没有这个这段</br>
 * 被设置为锁定的字段，若其值为true，则此对象不允许修改，其修改也不允许保存到数据库中</br>
 * 
 * @author 张弓 2023/6/15
 *
 */
@Target({ ElementType.FIELD })
@Retention(RUNTIME)
public @interface Locked {

}
