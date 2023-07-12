package cn.cerc.db.queue;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.queue.sqlmq.SqlmqGroup;
import cn.cerc.db.redis.Redis;

public class QueueGroup implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(QueueGroup.class);
    private String code;
    private int executionSequence = 1;
    private int total = 0;
    private int currentTotal = 0;
    private final int firstTotal;

    public QueueGroup(String code, int firstTotal) {
        this.code = code;
        this.firstTotal = firstTotal;
        if (firstTotal == 0)
            log.debug("consume message");
        else if (firstTotal > 1) {
            try (Redis redis = new Redis()) {
                redis.setex(this.code + 1, TimeUnit.DAYS.toSeconds(29), String.valueOf(firstTotal));
            }
        }
    }

    public String code() {
        return code;
    }

    public int executionSequence() {
        return executionSequence;
    }

    public int incr() {
        this.total++;
        return ++currentTotal;
    }

    public int next() {
        if (currentTotal == 0)
            throw new RuntimeException("当前行没有列数，不得进行下一行");
        currentTotal = 0;
        return ++executionSequence;
    }

    public int total() {
        return total;
    }

    @Override
    public void close() {
        if (this.firstTotal > 0 && this.total > 1)
            SqlmqGroup.updateGroupCode(this.code, this.total);
    }
}
