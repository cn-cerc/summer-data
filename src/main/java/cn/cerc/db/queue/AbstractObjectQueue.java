package cn.cerc.db.queue;

import com.google.gson.Gson;

public abstract class AbstractObjectQueue<T> extends AbstractQueue implements OnObjectMessage<T> {

    public abstract Class<T> getClazz();

    /**
     * 将dataRow发送到当前队列
     * 
     * @param object
     */
    public void append(T object) {
        super.sendMessage(getTopic(), getTag(), new Gson().toJson(object));
    }

    /**
     * 将dataRow发送到当前队列
     * 
     * @param object
     */
    public void append(String tag, T object) {
        super.sendMessage(getTopic(), tag, new Gson().toJson(object));
    }

    /**
     * 将dataRow发送到当前队列
     * 
     * @param object
     */
    public void append(String topic, String tag, T object) {
        super.sendMessage(topic, tag, new Gson().toJson(object));
    }

    @Override
    public boolean consume(String message) {
        T entity = new Gson().fromJson(message, getClazz());
        return this.execute(entity);
    }

    @Override
    public abstract boolean execute(T entity);
//
//    public boolean receive(OnObjectMessage<T> event) {
//        QueueConsumer consumer = QueueConsumer.getConsumer(this.getTopic(), this.getTag());
//        return consumer.receive(message -> event.execute(new Gson().fromJson(message, getClazz())));
//    }
}
