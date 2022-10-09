package cn.cerc.db.redis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPubSub;

public class Redis implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Redis.class);

    private redis.clients.jedis.Jedis jedis;

    public Redis() {
        this.jedis = JedisFactory.getJedis();
        if (jedis == null)
            log.warn("redis server 没有启动或无法连接");
    }

    public Redis(String configId) {
        this.jedis = JedisFactory.getJedis(configId);
        if (jedis == null)
            log.warn("redis server 没有启动或无法连接");
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
            jedis = null;
        }
    }

    public void expire(String key, final int seconds) {
        if (jedis != null)
            jedis.expire(key, seconds);
    }

    public void hdel(final String key, final String... fields) {
        if (jedis != null)
            jedis.hdel(key, fields);
    }

    public String hget(String key, String field) {
        return jedis != null ? jedis.hget(key, field) : null;
    }

    public Map<String, String> hgetAll(String key) {
        if (jedis != null)
            return jedis.hgetAll(key);
        else
            return new HashMap<String, String>();
    }

    public void hset(String key, String field, String value) {
        if (jedis != null)
            jedis.hset(key, field, value);
    }

    public void del(String key) {
        if (jedis != null)
            jedis.del(key);
    }

    public Set<String> hkeys(String key) {
        if (jedis != null)
            return jedis.hkeys(key);
        else
            return new HashSet<String>();
    }

    public void subscribe(final JedisPubSub jedisPubSub, final String... channels) {
        if (jedis != null)
            jedis.subscribe(jedisPubSub, channels);
    }

    public String get(String key) {
        if (jedis != null)
            return jedis.get(key);
        else
            return null;
    }

    public String getSet(String key, String value) {
        if (jedis != null)
            return jedis.getSet(key, value);
        else
            return null;
    }

    public Set<String> keys(String key) {
        if (jedis != null)
            return jedis.keys(key);
        else
            return new HashSet<String>();
    }

    public void rpush(final String key, final String... strings) {
        if (jedis != null)
            jedis.rpush(key, strings);
    }

    public void lpush(final String key, final String... strings) {
        if (jedis != null)
            jedis.lpush(key, strings);
    }

    public void publish(final String channel, final String message) {
        if (jedis != null)
            jedis.publish(channel, message);
    }

    public String rpop(String key) {
        if (jedis != null)
            return jedis.rpop(key);
        else
            return null;
    }

    public String scriptLoad(String luaScript) {
        if (jedis != null)
            return jedis.scriptLoad(luaScript);
        else
            return null;
    }

    public void evalsha(String sha1, List<String> keys, List<String> args) {
        if (jedis != null)
            jedis.evalsha(sha1, keys, args);
    }

    public void set(String key, String value) {
        if (jedis != null)
            jedis.set(key, value);
    }

    public void setex(final String key, final int seconds, final String value) {
        if (jedis != null)
            jedis.setex(key, seconds, value);
    }

    public long setnx(String key, String value) {
        if (jedis != null)
            return jedis.setnx(key, value);
        else
            return 0;
    }

    public static String getValue(String key) {
        try (Redis redis = new Redis()) {
            return redis.get(key);
        }
    }

    public static void setValue(String key, String value, int seconds) {
        try (Redis redis = new Redis()) {
            redis.setex(key, seconds, value);
        }
    }

    public static void setValue(String key, String value) {
        setValue(key, value, 3600);
    }

    public static void delete(String key) {
        try (Redis redis = new Redis()) {
            redis.del(key);
        }
    }

}
