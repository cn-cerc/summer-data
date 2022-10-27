package cn.cerc.db.queue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public abstract class AbstractObjectQueue<T> extends AbstractQueue {
    private static final Logger log = LoggerFactory.getLogger(AbstractObjectQueue.class);
    private Map<T, MessageView> items = new HashMap<>();

    public abstract Class<T> getClazz();

    /**
     * 将dataRow发送到当前队列
     * 
     * @param object
     * @throws ClientException
     */
    public void append(T object) {
        QueueServer.append(getTopic(), QueueConfig.tag, new Gson().toJson(object));
    }

    /**
     * 取出一条消息，类型为 DataRow，若没有则返回为null
     * 
     * @return DataRow
     */
    public T receive() {
        MessageView msg = consumer.recevie();
        if (msg == null)
            return null;
        try {
            T result = new Gson().fromJson(StandardCharsets.UTF_8.decode(msg.getBody()).toString(), getClazz());
            items.put(result, msg);
            return result;
        } catch (IllegalArgumentException | SecurityException e) {
            consumer.delete(msg);
            log.error(e.getMessage());
            log.error("{} 数据无法转换，已丢弃！", StandardCharsets.UTF_8.decode(msg.getBody()).toString());
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
        if (message != null) {
            consumer.delete(message);
            items.remove(object);
        }
    }

    /**
     * 一批取出多条消息，可通过dataSet.size()来判断实际取出了多少笔
     * 
     * @param maximum 最多取出多少笔，必须大于0
     */
    public List<T> receive(int maximum) {
        if (maximum <= 0)
            throw new RuntimeException("maximum 必须大于 0");

        List<T> list = new ArrayList<>();
        int total = 0;
        var msg = consumer.recevie();
        while (msg != null) {
            total++;
            String data = StandardCharsets.UTF_8.decode(msg.getBody()).toString();
            try {
                T result = new Gson().fromJson(data, getClazz());
                list.add(result);
                items.put(result, msg);
            } catch (IllegalArgumentException | SecurityException e) {
                consumer.delete(msg);
                log.error(e.getMessage());
                log.error("{} 数据无法转换，已丢弃！", data);
            }
            if (total == maximum)
                break;
            msg = consumer.recevie();
        }
        return list;
    }

}
