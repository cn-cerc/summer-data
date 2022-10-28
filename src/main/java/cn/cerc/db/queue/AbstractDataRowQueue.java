package cn.cerc.db.queue;

import cn.cerc.db.core.DataRow;

public abstract class AbstractDataRowQueue extends AbstractQueue {

    private transient Map<DataRow, Message> items = new HashMap<>();
    private transient Map<DataRow, MessageView> rmqItems = new HashMap<>();

    @Override
    public String getQueueId() {
        return this.getClass().getSimpleName();
    }

    /**
     * 生产者投放消息
     */
    public String append(DataRow dataRow) {
        return QueueServer.append(getTopic(), QueueConfig.tag, dataRow.json());
    }

}
