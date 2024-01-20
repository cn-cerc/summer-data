package cn.cerc.db.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.IRecord;

import java.util.concurrent.TimeUnit;

public class RedisRecord implements IRecord {
    private static final Logger log = LoggerFactory.getLogger(RedisRecord.class);

    public static final long TIMEOUT = TimeUnit.HOURS.toSeconds(1);

    private String key;
    private boolean existsData = false;
    private long expires = RedisRecord.TIMEOUT; // 单位：秒

    private final DataRow record = new DataRow();
    private boolean modified = false;

    // 缓存对象
    private boolean connected;

    public RedisRecord() {
        super();
    }

    public RedisRecord(Class<?> clazz) {
        super();
        this.setKey(clazz.getName());
    }

    public RedisRecord(Object... keys) {
        super();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) {
                builder.append(".");
            }
            builder.append(keys[i]);
        }
        setKey(builder.toString());
    }

    public final void post() {
        if (this.modified) {
            try {
                Redis.setValue(key, record.toString(), this.expires);
                log.debug("cache set:" + key + ":" + record.json());
                this.modified = false;
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    public boolean isNull() {
        return !this.existsData;
    }

//    @Deprecated
//    public void setNull(String field) {
//        setValue(field, null);
//    }

    public String getKey() {
        return key;
    }

    public RedisRecord setKey(String key) {
        if (this.key != null) {
            throw new RuntimeException("[CacheQuery] param init error");
        }
        if (key == null) {
            throw new RuntimeException("[CacheQuery] param init error");
        }
        this.key = key;

        connected = true;
        existsData = false;
        String recordStr = Redis.getValue(key);
        log.debug("cache get: {} - {}", key, recordStr);
        if (recordStr != null && !recordStr.isEmpty()) {
            try {
                record.setJson(recordStr);
                existsData = true;
            } catch (Exception e) {
                log.error("cache data error {}, error {}", recordStr, e.getMessage(), e);
            }
        }
        return this;
    }

    public void clear() {
        if (this.existsData) {
            // log.info("cache delete:" + key.toString());
            Redis.delete(key);
            this.existsData = false;
        }
        record.clear();
        record.fields().clear();
        this.modified = false;
    }

    public boolean hasValue(String field) {
        return !isNull() && getString(field) != null && !"".equals(getString(field)) && !"{}".equals(getString(field));
    }

    public long getExpires() {
        return expires;
    }

    public void setExpires(long expires) {
        this.expires = expires;
    }

    public boolean Connected() {
        return connected;
    }

//    @Deprecated
//    public Datetime getDateTime(String field) {
//        return record.getDatetime(field);
//    }

    @Override
    public RedisRecord setValue(String field, Object value) {
        this.modified = true;
        record.setValue(field, value);
        return this;
    }

//    @Deprecated
//    public RedisRecord setField(String field, Object value) {
//        return setValue(field, value);
//    }

    @Override
    public String toString() {
        return record.toString();
    }

    public DataRow getRecord() {
        return this.record;
    }

    @Override
    public boolean exists(String field) {
        return record.exists(field);
    }

    @Override
    public Object getValue(String field) {
        return record.getValue(field);
    }

//    @Deprecated
//    public Object getField(String field) {
//        return getValue(field);
//    }

}
