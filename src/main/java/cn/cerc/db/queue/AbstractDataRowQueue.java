package cn.cerc.db.queue;

import cn.cerc.db.core.DataRow;

public abstract class AbstractDataRowQueue extends AbstractQueue implements OnReceiveDataRow {

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

    public boolean receive(OnReceiveDataRow event) {
        QueueConsumer consumer = QueueConsumer.getConsumer(this.getTopic(), this.getTag());
        return consumer.receive(data -> event.execute(new DataRow().setJson(data)));
    }

}
