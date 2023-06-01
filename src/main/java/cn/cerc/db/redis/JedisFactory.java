package cn.cerc.db.redis;

import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Jedis 工厂调度类，支持项目加载不同的配置
 */
public class JedisFactory {
    private static final Map<String, JedisBuilder> items = new ConcurrentHashMap<>();
    private static final JedisFactory instance = new JedisFactory();

    private JedisFactory() {
        // 私有构造函数，防止外部实例化
    }

    /**
     * 返回默认RedisServer的Jedis
     *
     * @return Jedis
     */
    public static Jedis getJedis() {
        return instance.get("");
    }

    /**
     * 返回 RedisServer 的 Jedis
     *
     * @param configId 用于在配置文件中区分不同的redis服务器的连接参数，取值如：sync，若为 null 则返回缺省配置
     * @return Jedis
     */
    public static Jedis getJedis(String configId) {
        return instance.get(configId);
    }

    public static Redis getRedis() {
        return new Redis();
    }

    public static Redis getRedis(String configId) {
        return new Redis(configId);
    }

    /**
     * 创建 JedisFactory
     *
     * @param configId 用于在配置文件中区分不同的redis服务器的连接参数，取值如：sync，若为 null 则返回缺省配置
     * @return JedisFactory
     */
    private Jedis get(String configId) {
        if (items.containsKey(configId))
            return items.get(configId).getResource();

        JedisBuilder builder = JedisBuilder.getInstance(configId);
        items.put(configId, builder);
        return builder.getResource();
    }

    /**
     * 销毁所有的 redis 连接
     */
    public static void destroy() {
        items.values().forEach(JedisBuilder::close);
    }

}
