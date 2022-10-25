package cn.cerc.db.queue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.model.Message;
import com.google.gson.Gson;

public abstract class AbstractObjectQueue<T> extends AbstractQueue {
    private static final Logger log = LoggerFactory.getLogger(AbstractObjectQueue.class);
    private Map<T, Message> items = new HashMap<>();
    
    public abstract Class<T> getClazz();

    /**
     * 将dataRow发送到当前队列
     * 
     * @param object
     */
    public void append(T object) {
        Message message = new Message();
        message.setMessageBody(new Gson().toJson(object));
        getQueue().putMessage(message);
    }

    /**
     * 取出一条消息，类型为 DataRow，若没有则返回为null
     * 
     * @return DataRow
     */
    public T receive() {
        Message msg = this.popMessage();
        if (msg == null)
            return null;

        String data = getMessageBody(msg);
        try {
            T result = new Gson().fromJson(data, getClazz());
            items.put(result, msg);
            return result;
        } catch (IllegalArgumentException | SecurityException e) {
            this.getQueue().deleteMessage(msg.getReceiptHandle());
            log.error(e.getMessage());
            log.error("{} 数据无法转换，已丢弃！", data);
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除一条消息，要删除的消息必须是被当前对象所取出来的
     * 
     * @param dataRow
     */
    public void delete(T object) {
        if (!items.containsKey(object))
            throw new RuntimeException("object not find!");
        var message = items.get(object);
        if (message != null)
            getQueue().deleteMessage(message.getReceiptHandle());
    }

    /**
     * 一批取出多条消息，可通过dataSet.size()来判断实际取出了多少笔
     * 
     * @param maximum 最多取出多少笔，必须大于0
     * @return DataSet
     */
    public List<T> receive(int maximum) {
        if (maximum <= 0)
            throw new RuntimeException("maximum 必须大于 0");
        List<T> list = new ArrayList<>();
        int total = 0;
        Message msg = this.popMessage();
        while (msg != null) {
            total++;
            String data = getMessageBody(msg);
            try {
                T result = new Gson().fromJson(data, getClazz());
                list.add(result);
                items.put(result, msg);
            } catch (IllegalArgumentException | SecurityException e) {
                this.getQueue().deleteMessage(msg.getReceiptHandle());
                log.error(e.getMessage());
                log.error("{} 数据无法转换，已丢弃！", data);
            }
            if (total == maximum)
                break;
            msg = this.popMessage();
        }
        return list;
    }

}
