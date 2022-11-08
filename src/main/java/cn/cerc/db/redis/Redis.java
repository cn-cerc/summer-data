package cn.cerc.db.redis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPubSub;

public class Redis implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Redis.class);
    private static AtomicBoolean noRedis = new AtomicBoolean(false);
    private static Map<String, RedisData> items = new ConcurrentHashMap<>();
    private static LinkedBlockingDeque<String> queueServer = new LinkedBlockingDeque<>();
    private redis.clients.jedis.Jedis jedis;

    public Redis() {
        if (noRedis.get())
            return;
        this.jedis = JedisFactory.getJedis();
        if (jedis == null) {
            log.error("redis server 没有启动或无法连接，将改为以单机模式运行");
            noRedis.set(true);
        }
    }

    public Redis(String configId) {
        if (noRedis.get())
            return;
        this.jedis = JedisFactory.getJedis(configId);
        if (jedis == null) {
            log.warn("redis server 没有启动或无法连接");
            noRedis.set(true);
        }
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
        else {
            var item = items.get(key);
            if (item != null)
                item.setExpire(seconds);
        }
    }

    public void hdel(final String key, final String... fields) {
        if (jedis != null)
            jedis.hdel(key, fields);
        else
            items.remove(key);
    }

    public String hget(String key, String field) {
        if (jedis != null) {
            return jedis.hget(key, field);
        } else {
            RedisData item = items.get(key);
            if (item != null)
                return item.hget(field);
            else
                return null;
        }
    }

    public Map<String, String> hgetAll(String key) {
        if (jedis != null)
            return jedis.hgetAll(key);
        else {
            RedisData item = items.get(key);
            if (item != null)
                return item.hgetAll();
            else
                return new HashMap<String, String>();
        }
    }

    public void hset(String key, String field, String value) {
        if (jedis != null)
            jedis.hset(key, field, value);
        else {
            var item = items.get(key);
            if (item == null) {
                item = new RedisData(key, "");
                items.put(key, item);
            }
            item.hset(field, value);
        }
    }

    public void del(String key) {
        if (jedis != null)
            jedis.del(key);
        else
            items.remove(key);
    }

    public Set<String> hkeys(String key) {
        if (jedis != null)
            return jedis.hkeys(key);
        else {
            var item = items.get(key);
            if (item != null)
                return item.hgetAll().keySet();
            else
                return new HashSet<String>();
        }
    }

    public void subscribe(final JedisPubSub jedisPubSub, final String... channels) {
        if (jedis != null)
            jedis.subscribe(jedisPubSub, channels);
        else
            log.error("单机模式下，不支持订阅消息");
    }

    public void publish(final String channel, final String message) {
        if (jedis != null)
            jedis.publish(channel, message);
        else
            log.error("单机模式下，不支持发布消息");
    }

    public String get(String key) {
        if (jedis != null)
            return jedis.get(key);
        else {
            var item = items.get(key);
            if (item != null)
                return item.value();
            else
                return null;
        }
    }

    public String getSet(String key, String value) {
        if (jedis != null)
            return jedis.getSet(key, value);
        else {
            var item = items.get(key);
            if (item == null) {
                item = new RedisData(key, value);
                items.put(key, item);
                return value;
            } else
                return item.value();
        }
    }

    public Set<String> keys(String key) {
        if (jedis != null)
            return jedis.keys(key);
        else {
            var result = new HashSet<String>();
            for (String item : items.keySet()) {
                if (item.startsWith(key))
                    result.add(item);
            }
            return result;
        }
    }

    public void lpush(final String key, final String... strings) {
        if (jedis != null)
            jedis.lpush(key, strings);
        else {
            for (String value : strings)
                queueServer.add(value);
        }
    }

    public void rpush(final String key, final String... strings) {
        if (jedis != null)
            jedis.rpush(key, strings);
        else {
            for (String value : strings)
                queueServer.addFirst(value);
        }
    }

    public String rpop(String key) {
        if (jedis != null)
            return jedis.rpop(key);
        else
            return queueServer.pop();
    }

    public String scriptLoad(String luaScript) {
        if (jedis != null)
            return jedis.scriptLoad(luaScript);
        else {
            log.error("单机模式下，不支持redis脚本");
            return null;
        }
    }

    public void evalsha(String sha1, List<String> keys, List<String> args) {
        if (jedis != null)
            jedis.evalsha(sha1, keys, args);
        else
            log.error("单机模式下，不支持redis脚本");
    }

    public void set(String key, String value) {
        if (jedis != null)
            jedis.set(key, value);
        else {
            var item = items.get(key);
            if (item == null)
                items.put(key, new RedisData(key, value));
            else
                item.setValue(value);
        }
    }

    public long setnx(String key, String value) {
        if (jedis != null)
            return jedis.setnx(key, value);
        else {
            var item = items.get(key);
            if (item == null) {
                items.put(key, new RedisData(key, value));
                return 1;
            } else
                return 0;
        }
    }

    /**
     * 设置key和value，且一并设置过期时间
     */
    public void setex(final String key, final int seconds, final String value) {
        if (jedis != null)
            jedis.setex(key, seconds, value);
        else {
            var item = items.get(key);
            if (item == null)
                items.put(key, new RedisData(key, value).setExpire(seconds));
            else
                item.setValue(value).setExpire(seconds);
        }
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
