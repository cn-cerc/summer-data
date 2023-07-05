package cn.cerc.db.redis;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisData {
    private String key;
    private String value;
    private long timestamp;
    private Map<String, String> items;

    public RedisData(String key, String value) {
        super();
        this.key = key;
        this.value = value;
    }

    public String value() {
        if (timestamp > 0 && timestamp < System.currentTimeMillis()) {
            System.out.println("过期！！！！！！！！！！");
            Redis.delete(key);
            return null;
        }
        return value;
    }

    public String hget(String field) {
        return items().get(field);
    }

    public Map<String, String> hgetAll() {
        return items();
    }

    public RedisData setExpire(long seconds) {
        timestamp = System.currentTimeMillis() + seconds * 1000;
        return this;
    }

    public RedisData setValue(String value) {
        this.value = value;
        return this;
    }

    public void hset(String field, String value) {
        items().put(field, value);
    }

    private Map<String, String> items() {
        synchronized (this) {
            if (items == null)
                items = new ConcurrentHashMap<>();
        }
        return items;
    }

}
