package cn.cerc.db.queue;

import cn.cerc.db.core.DataRow;

import java.time.Duration;

public abstract class AbstractDataRowQueue extends AbstractQueue {

    /**
     * 生产者投放消息
     */
    public String append(DataRow dataRow) {
        return QueueServer.append(getTopic(), QueueConfig.tag, dataRow.json(), Duration.ofSeconds(0));
    }

}
