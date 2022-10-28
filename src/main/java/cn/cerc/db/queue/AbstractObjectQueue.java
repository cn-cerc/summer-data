package cn.cerc.db.queue;

import com.google.gson.Gson;

public abstract class AbstractObjectQueue<T> extends AbstractQueue {

    public abstract Class<T> getClazz();

    /**
     * 将dataRow发送到当前队列
     * 
     * @param object
     */
    public void append(T object) {
        QueueServer.append(getTopic(), QueueConfig.tag, new Gson().toJson(object),delayTime());
    }

}
