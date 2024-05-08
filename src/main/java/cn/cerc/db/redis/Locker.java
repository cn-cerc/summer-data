package cn.cerc.db.redis;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.SummerDB;
import cn.cerc.db.core.ClassResource;
import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.Utils;

public class Locker implements Closeable {
    private static final ClassResource res = new ClassResource(Locker.class, SummerDB.ID);
    private static final Logger log = LoggerFactory.getLogger(Locker.class);

    private String group;
    private String message;
    private Map<String, Boolean> items = new HashMap<>();
    private int timeout = 10000; // 锁超时时间，默认10秒
    private boolean locked; // 加锁状态
    private String description;

    /**
     * 创建一把锁，最终形成锁Key：group-child
     * 
     * @param group 锁前缀, 一般为类名
     * @param child 锁后缀
     */
    public Locker(String group, String child) {
        this.group = group;
        items.put(group + "-" + child, false);
    }

    @Deprecated // 多把锁请使用另一个同名函数
    public Locker(String group, Object firstChild, Object... otherChildren) {
        this.group = group;
        items.put(group + "-" + firstChild, false);
        for (Object child : otherChildren) {
            items.put(group + "-" + child, false);
        }
    }

    /**
     * 创建多把锁，最终形成的锁类似于：group-1, group-2, group-3
     * 
     * @param group
     * @param children
     */
    public Locker(String group, Set<String> children) {
        if (Utils.isEmpty(children))
            throw new RuntimeException("创建多把锁失败，set为空");
        this.group = group;
        for (String child : children)
            items.put(group + "-" + child, false);
    }

    @Deprecated
    public void add(Object key) {
        items.put(group + "-" + key, false);
    }

    @Deprecated
    public boolean lock(String flag) {
        return lock(flag, 100);
    }

    /**
     * 请求申请分布式锁，请改使用 requestLock
     * 
     * @param description     请求加锁后要处理的业务内容描述，特别注意不要在其中使用逗号!
     * @param maximumWaitTime 请求加锁时间，单位为毫秒，此值必须为100的倍数
     * @return 加锁成功返为true，否则为false
     */
    @Deprecated
    public boolean lock(String description, int maximumWaitTime) {
        return requestLock(description, maximumWaitTime);
    }

    /**
     * 请求申请分布式锁
     * 
     * @param description     请求加锁后要处理的业务内容描述，特别注意不要在其中使用逗号!
     * @param maximumWaitTime 请求加锁时间，单位为毫秒，此值必须为100的倍数
     * @return 加锁成功返为true，否则为false
     */
    public boolean requestLock(String description, int maximumWaitTime) {
        this.description = description;
        if (maximumWaitTime % 100 != 0) {
            throw new RuntimeException(String.format("%s %% 100 !=0", maximumWaitTime));
        }
        if (items.size() == 0) {
            items.put(group, false);
        }
        if (log.isDebugEnabled())
            log.info("{} 申请加锁", this.description);
        try (Redis redis = new Redis()) {
            for (String key : items.keySet()) {
                if (!tryLock(redis, key, description, maximumWaitTime / 100)) {
                    log.warn(this.message);
                    return false;
                }
                items.put(key, true);
            }
            this.locked = true;
            if (log.isDebugEnabled())
                log.info("{} 加锁成功", this.description);
            return true;
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    private boolean tryLock(Redis redis, String key, String jobDescription, int num) throws InterruptedException {
        boolean result = false;
        int i = 0;
        while (i < num) {
            i++;
            long curTime = System.currentTimeMillis() + timeout;
            if (redis.setnx(key, curTime + "," + jobDescription) == 1) {
                redis.expire(key, this.timeout);
                this.message = String.format(res.getString(1, "%s锁定成功"), jobDescription);
                result = true;
                break;
            } else {
                String currentValue = redis.get(key);
                if (currentValue != null && currentValue.split(",").length == 2) {
                    String[] args = currentValue.split(",");
                    long lastTime = Long.parseLong(args[0]);
                    if (System.currentTimeMillis() > lastTime) {
                        String oldValue = redis.getSet(key, curTime + "," + jobDescription);
                        if (oldValue != null && oldValue.equals(currentValue)) {
                            this.message = String.format(res.getString(2, "%s强制锁定成功"), jobDescription);
                            log.error(this.message);
                            result = true;
                            break;
                        }
                    }
                    Datetime tmp = new Datetime(lastTime);
                    this.message = String.format(res.getString(3, "%s锁定失败， 待%s完成后(%s)再试"), jobDescription, args[1],
                            tmp.getTime());
                    if (log.isDebugEnabled())
                        log.debug(this.message);
                } else {
                    this.message = String.format(res.getString(4, "%s锁定失败， %s完成后再试"), jobDescription, currentValue);
                    log.warn(this.message);
                }
            }
            if (i < num) {
                Thread.sleep(100);
            }
        }
        return result;
    }

    /**
     * 遇到超长业务执行时，可执行此任务进行延长时间
     */
    public void renewal() {
        if (this.locked()) {
            String newValue = (System.currentTimeMillis() + timeout) + "," + this.description;
            try (Redis redis = new Redis()) {
                for (String key : items.keySet()) {
                    redis.set(key, newValue);
                    redis.expire(key, this.timeout);
                }
            }
        }
    }

    @Override
    public void close() {
        if (this.locked) {
            try (Redis redis = new Redis()) {
                for (String key : items.keySet()) {
                    if (items.get(key)) {
                        redis.del(key);
                    }
                }
                this.locked = false;
            }
            if (log.isDebugEnabled())
                log.info("{} 执行完成", this.description);
        }
    }

    public String description() {
        return this.description;
    }

    /**
     * 
     * @return 返回提示讯息
     */
    public String message() {
        return message;
    }

    /**
     * 请改使用 message 函数
     * 
     * @return 返回提示讯息
     */
    @Deprecated
    public String getMessage() {
        return message();
    }

    public int timeout() {
        return timeout;
    }

    /**
     * 请改使用 timeout 函数
     * 
     * @return
     */
    @Deprecated
    public int getTimeout() {
        return timeout();
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public boolean locked() {
        return this.locked;
    }

}
