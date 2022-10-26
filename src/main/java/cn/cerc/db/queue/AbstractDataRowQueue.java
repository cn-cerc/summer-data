package cn.cerc.db.queue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.DataSet;

public abstract class AbstractDataRowQueue extends AbstractQueue {

    private transient Map<DataRow, MessageView> items = new HashMap<>();

    /**
     * 生产者投放消息
     */
    public String append(DataRow dataRow) {
        return QueueServer.append(getTopic(), QueueConfig.tag, dataRow.json());
    }

    /**
     * 消费者消费消息
     * 
     * 取出一条消息，类型为 DataRow，若没有则返回为null
     */
    public DataRow receive() {
        MessageView msg = consumer.recevie();
        if (msg == null)
            return null;
        DataRow record = new DataRow();
        record.setJson(StandardCharsets.UTF_8.decode(msg.getBody()).toString());
        items.put(record, msg);
        return record;
    }

    /**
     * 删除一条消息，要删除的消息必须是被当前对象所取出来的
     * 
     * @param dataRow
     * @throws ClientException
     */
    public void delete(DataRow dataRow) throws Exception {
        if (!items.containsKey(dataRow))
            throw new RuntimeException("dataRow not find!");
        var message = items.get(dataRow);
        if (message != null) {
            consumer.delete(message);
            items.remove(dataRow);
        }
    }

    /**
     * 一批取出多条消息，可通过dataSet.size()来判断实际取出了多少笔
     * 
     * @param maximum 最多取出多少笔，必须大于0
     * @return DataSet
     * @throws Exception
     */
    public DataSet receive(int maximum) throws Exception {
        if (maximum <= 0)
            throw new RuntimeException("maximum 必须大于 0");
        DataSet dataSet = new DataSet();
        int total = 0;
        var msg = consumer.recevie();
        while (msg != null) {
            total++;
            var row = dataSet.append().current();
            row.setJson(StandardCharsets.UTF_8.decode(msg.getBody()).toString());
            items.put(row, msg);
            if (total == maximum)
                break;
            msg = consumer.recevie();
        }
        return dataSet;
    }

}
