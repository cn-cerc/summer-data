package cn.cerc.db.queue;

import cn.cerc.db.core.DataRow;

public abstract class AbstractDataRowQueue extends AbstractQueue {

    /**
     * 生产者投放消息
     */
    public String append(DataRow dataRow) {
        return super.sendMessage(dataRow.json());
    }

    @Override
    public boolean consume(String message) {
        return this.execute(new DataRow().setJson(message));
    }

    public abstract boolean execute(DataRow data);

}
