package cn.cerc.core;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Target(TYPE)
@Retention(RUNTIME)
public @interface EntityKey {

    /**
     * @return 设置entity的业务keyField
     */
    String[] fields();

    /**
     * @return 在get entity时，key值是否默认加入公司别
     */
    boolean corpNo() default false;

    /**
     * @return 返回缓存的版本号
     */
    int version() default 0;

    /**
     * @return 返回缓存的等级，默认为关闭
     */
    CacheLevelEnum cache() default CacheLevelEnum.Disabled;

    /**
     * @return 缓存过期时间，单位为秒
     */
    int expire() default 3600;

    /**
     * @return 是否虚拟对象，若为虚拟对象则必须自行实现EntityCacheImpl接口
     */
    boolean virtual() default false;

}
