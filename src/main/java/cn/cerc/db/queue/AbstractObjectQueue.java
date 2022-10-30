package cn.cerc.db.queue;

import com.google.gson.Gson;

public abstract class AbstractObjectQueue<T> extends AbstractQueue implements OnReceiveObject<T> {

    public abstract Class<T> getClazz();

    /**
     * 将dataRow发送到当前队列
     * 
     * @param object
     */
    public void append(T object) {
        super.sendMessage(new Gson().toJson(object));
    }

    @Override
    public boolean consume(String message) {
        T entity = new Gson().fromJson(message, getClazz());
        return this.execute(entity);
    }

    @Override
    public abstract boolean execute(T entity);

    public boolean receive(OnReceiveObject<T> event) {
        QueueConsumer consumer = QueueConsumer.getConsumer(this.getTopic(), this.getTag());
        return consumer.receive(message -> event.execute(new Gson().fromJson(message, getClazz())));
    }
}
