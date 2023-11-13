package cn.cerc.db.redis;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.SummerDB;
import cn.cerc.db.core.ClassResource;
import redis.clients.jedis.Jedis;

public class Locker implements Closeable {
    private static final ClassResource res = new ClassResource(Locker.class, SummerDB.ID);
    private static final Logger log = LoggerFactory.getLogger(Locker.class);

    private String group;
    private String message;
    private Map<String, Boolean> items = new HashMap<>();
    private int timeout = 10000; // 锁超时时间，默认10秒

    public Locker(String group, Object first, Object... args) {
        this.group = group;
        items.put(group + "-" + first, false);
        for (Object arg : args)
            items.put(group + "-" + arg, false);
    }
    
    /**
     * 默认三秒释放锁
     */
    public boolean lock() {
        return lock("", 3000);
    }
    
    public boolean lock(String flag, int time) {
        if (time % 100 != 0)
            throw new RuntimeException(String.format("%s %% 100 !=0", time));
        if (items.size() == 0)
            items.put(group, false);
        try (Jedis jedis = JedisFactory.getJedis()) {
            for (String key : items.keySet()) {
                if (!tryLock(jedis, key, flag, time)) {
                    log.warn(this.message);
                    return false;
                }
                items.put(key, true);
            }
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean tryLock(Jedis jedis, String key, String flag, int time) throws InterruptedException {
        int i = 0;
        int totalSleepTime = 0;
        // 重试机制 递增睡眠时间
        while (totalSleepTime < time) {
            if (jedis.setnx(key, System.currentTimeMillis() + "," + flag) == 1) {
                this.message = String.format(res.getString(1, "[%s]%s锁定成功"), key, flag);
                // 设置到期时间，避免服务器更新时，为能正常释放key，导致死锁
                jedis.expire(key, this.timeout);
                return true;
            }
            int sleepTime = i++ * 100;
            totalSleepTime += sleepTime;
            Thread.sleep(sleepTime);
        }
        this.message = String.format(res.getString(4, "[%s]%s锁定失败"), key, flag);
        return false;
    }

    @Override
    public void close() {
        try (Jedis jedis = JedisFactory.getJedis()) {
            for (String key : items.keySet()) {
                if (items.get(key))
                    jedis.del(key);
            }
        }
    }

    public Locker add(Object key) {
        items.put(group + "-" + key, false);
        return this;
    }

    public int getTimeout() {
        return timeout;
    }

    public Locker setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public String getMessage() {
        return message;
    }

}
