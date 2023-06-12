package cn.cerc.db.redis;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.Utils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * 不同配置的 redis 连接池构造器
 */
public class JedisBuilder {
    private static final Logger log = LoggerFactory.getLogger(JedisBuilder.class);
    // 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
    private static final int MAX_ACTIVE = 1024;
    // 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
    private static final int MAX_IDLE = 200;
    // 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
    private static final int MAX_WAIT = 10000;
    // 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
    private static final boolean TEST_ON_BORROW = true;

    private JedisPool jedisPool;
    private final String host;
    private final int port;
    private final AtomicInteger atomic = new AtomicInteger();

    private static final Map<String, JedisBuilder> builders = new ConcurrentHashMap<>();

    public static JedisBuilder getInstance(String configId) {
        return builders.computeIfAbsent(configId, JedisBuilder::new);
    }

    private JedisBuilder(String configId) {
        RedisConfig config = new RedisConfig(configId);
        this.host = config.host();
        this.port = config.port();
        if (Utils.isEmpty(this.host)) {
            log.warn("{} 项目节点没有配置 redis.host，系统将运行于单机模式", config.getFullPath());
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
        if (Utils.isEmpty(password))
            password = null;
        int timeout = config.timeout();

        // 创建 JedisPool 连接池
        jedisPool = new JedisPool(poolConfig, host, port, timeout, password);
        log.info("{}:{} redis server connected", host, port);
    }

    /**
     * 从 JedisPool 获取 Jedis 实例
     *
     * @return jedis
     */
    public Jedis getResource() {
        // 连接池未创建返回空
        if (jedisPool == null) {
            log.error("{}:{} jedis pool is empty", host, port);
            return null;
        }

        // 连接池若关闭返回空
        if (jedisPool.isClosed()) {
            log.info("{}:{} jedis pool is closed", host, port);
            return null;
        }

        // 达3次时，不再重试
        if (this.atomic.get() >= 3) {
            log.error("{}:{} redis 尝试连接 {} 次失败，不再进行尝试", this.host, this.port, this.atomic.get());
            return null;
        }

        try {
            return jedisPool.getResource();
        } catch (JedisConnectionException e) {
            if (this.atomic.get() < 3) {
                log.error("redis {}:{} 无法联接，原因：{}", this.host, this.port, e.getMessage());
                this.atomic.incrementAndGet();
            }
            return null;
        }
    }

    public void close() {
        this.jedisPool.close();
    }

}
