package cn.cerc.db.queue;

import cn.cerc.db.core.DataRow;

public abstract class AbstractDataRowQueue extends AbstractQueue implements OnMessageDataRow {

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

    @Override
    public abstract boolean execute(DataRow data);
//
//    public boolean receive(OnMessageDataRow event) {
//        QueueConsumer consumer = new QueueConsumer();
//        return consumer.receive("tempGroup", this.getTopic(), this.getTag(), data -> event.execute(new DataRow().setJson(data)));
//    }

}
