package cn.cerc.db.core;

public enum CacheLevelEnum {
    /**
     * 关闭缓存
     */
    Disabled,
    /**
     * 1级缓存：开通Redis缓存
     */
    Redis,
    /**
     * 2级缓存：开通Redis与Session缓存
     */
    RedisAndSession,
	/**
	 * 3级缓存：使用EntitySpace
	 */
    EntitySpace;
}
