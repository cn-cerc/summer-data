package cn.cerc.db.redis;

import cn.cerc.db.core.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class JedisConfig {
    private static final Logger log = LoggerFactory.getLogger(JedisConfig.class);
    private static final Map<String, JedisConfig> items = new HashMap<>();
    // 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
    private static final int MAX_ACTIVE = 1024;
    // 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
    private static final int MAX_IDLE = 200;
    // 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
    private static final int MAX_WAIT = 10000;
    // 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
    private static final boolean TEST_ON_BORROW = true;

    // redis pool
    private JedisPool jedisPool = null;
    private final String host;
    private final int port;
    private final AtomicInteger error = new AtomicInteger();

    /**
     * 创建默认的 RedisServer
     *
     * @return JedisFactory
     */
    public static JedisConfig create() {
        return create(null);
    }

    /**
     * 创建 JedisFactory
     *
     * @param configId 用于在配置文件中区分不同的redis服务器的连接参数，取值如：sync，若为 null 则返回缺省配置
     * @return JedisFactory
     */
    public static JedisConfig create(String configId) {
        if (items.containsKey(configId)) {
            return items.get(configId);
        }
        synchronized (JedisConfig.class) {
            if (items.containsKey(configId))
                return items.get(configId);
            JedisConfig item = new JedisConfig(configId);
            items.put(configId, item);
            return item;
        }
    }

    /**
     * 返回 RedisServer 的 Jedis
     *
     * @param configId 用于在配置文件中区分不同的redis服务器的连接参数，取值如：sync，若为 null 则返回缺省配置
     * @return Jedis
     */
    public static Jedis getJedis(String configId) {
        return create(configId).getResource();
    }

    public static Redis getRedis(String configId) {
        return new Redis(configId);
    }

    /**
     * 返回默认RedisServer的Jedis
     *
     * @return Jedis
     */
    public static Jedis getJedis() {
        return create(null).getResource();
    }

    public static Redis getRedis() {
        return new Redis();
    }

    private JedisConfig(String configId) {
        RedisConfig config = new RedisConfig(configId);
        this.host = config.host();
        this.port = config.port();
        if (Utils.isEmpty(this.host)) {
            log.warn("当前项目没有配置redis.host，系统将运行于单机模式");
            return;
        }

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(MAX_ACTIVE);
        poolConfig.setMaxIdle(MAX_IDLE);
        poolConfig.setMaxWaitMillis(MAX_WAIT);
        poolConfig.setTestOnBorrow(TEST_ON_BORROW);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        // Idle时进行连接扫描
        poolConfig.setTestWhileIdle(true);
        // 表示idle object evitor两次扫描之间要sleep的毫秒数
        poolConfig.setTimeBetweenEvictionRunsMillis(30000);
        // 表示idle object evitor每次扫描的最多的对象数
        poolConfig.setNumTestsPerEvictionRun(10);
        // 表示一个对象至少停留在idle状态的最短时间，然后才能被idle object
        // evitor扫描并驱逐；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义
        poolConfig.setMinEvictableIdleTimeMillis(60000);

        if (Utils.isEmpty(host)) {
            log.error("{}/host not config.", config.getFullPath());
            return;
        }

        String password = config.password();
        if ("".equals(password))
            password = null;
        int timeout = config.timeout();

        // 建立连接池
        jedisPool = new JedisPool(poolConfig, host, port, timeout, password);
        log.info("{}:{} redis server connected", host, port);
    }

    public Jedis getResource() {
        if (jedisPool == null) {
            log.error("{}:{} jedis pool not exist", host, port);
            return null;
        }
        if (jedisPool.isClosed()) {
            log.error("{}:{} jedis pool is closed", host, port);
            return null;
        }
        // 达3次时，不再重试
        if (this.error.get() >= 3) {
            log.error("redis {}:{} 尝试连接 {} 次失败，不在进行尝试", this.host, this.port, this.error.get());
            return null;
        }
        try {
            return jedisPool.getResource();
        } catch (JedisConnectionException e) {
            if (this.error.get() < 3) {
                log.error("redis {}:{} 无法联接，原因：{}", this.host, this.port, e.getMessage());
                this.error.incrementAndGet();
            }
            return null;
        }
    }

    public static void close() {
        items.values().forEach((factory) -> factory.jedisPool.close());
    }

}
