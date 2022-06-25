package cn.cerc.db.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.IRecord;

public class RedisRecord implements IRecord {
    private static final Logger log = LoggerFactory.getLogger(RedisRecord.class);

    public static final int TIMEOUT = 3600;

    private String key;
    private boolean existsData = false;
    private int expires = RedisRecord.TIMEOUT; // 单位：秒

    private DataRow record = new DataRow();
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
        StringBuffer str = new StringBuffer();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) {
                str.append(".");
            }
            str.append(keys[i]);
        }
        setKey(str.toString());
    }

    public final void post() {
        if (this.modified) {
            try {
                Redis.set(key, record.toString(), this.expires);
                log.debug("cache set:" + key + ":" + record.toString());
                this.modified = false;
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    public boolean isNull() {
        return !this.existsData;
    }

    @Deprecated
    public void setNull(String field) {
        setValue(field, null);
    }

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
        String recordStr = Redis.get(key);
        log.debug("cache get: {} - {}", key, recordStr);
        if (recordStr != null && !"".equals(recordStr)) {
            try {
                record.setJson(recordStr);
                existsData = true;
            } catch (Exception e) {
                log.error("cache data error：" + recordStr, e);
                e.printStackTrace();
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

    public int getExpires() {
        return expires;
    }

    public void setExpires(int expires) {
        this.expires = expires;
    }

    public boolean Connected() {
        return connected;
    }

    @Deprecated
    public Datetime getDateTime(String field) {
        return record.getDatetime(field);
    }

    @Override
    public RedisRecord setValue(String field, Object value) {
        this.modified = true;
        record.setValue(field, value);
        return this;
    }

    @Deprecated
    public RedisRecord setField(String field, Object value) {
        return setValue(field, value);
    }

    @Override
    public String toString() {
        if (record != null) {
            return record.toString();
        } else {
            return null;
        }
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

    @Deprecated
    public Object getField(String field) {
        return getValue(field);
    }

}
