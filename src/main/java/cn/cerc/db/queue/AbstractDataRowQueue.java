package cn.cerc.db.queue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;

import com.aliyun.mns.model.Message;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.DataSet;

public abstract class AbstractDataRowQueue extends AbstractQueue {

    private transient Map<DataRow, Message> items = new HashMap<>();
    private transient Map<DataRow, MessageView> rmqItems = new HashMap<>();

    /**
     * 将dataRow发送到当前队列
     * 
     * @param dataRow
     * @throws ClientException
     */
    public String append(DataRow dataRow) throws ClientException {
        if (rmqQueue == null) {
            Message message = new Message();
            message.setMessageBody(dataRow.json());
            return getQueue().putMessage(message).getMessageId();
        } else {
            return rmqQueue.producer().append(dataRow.json());
        }
    }

    /**
     * 取出一条消息，类型为 DataRow，若没有则返回为null
     * 
     * @return DataRow
     * @throws Exception
     */
    public DataRow receive() throws Exception {
        if (rmqQueue == null) {
            Message msg = this.popMessage();
            if (msg == null)
                return null;
            DataRow result = new DataRow();
            result.setJson(getMessageBody(msg));
            items.put(result, msg);
            return result;
        } else {
            MessageView msg = rmqQueue.consumer().recevie();
            if (msg == null)
                return null;
            DataRow result = new DataRow();
            result.setJson(StandardCharsets.UTF_8.decode(msg.getBody()).toString());
            rmqItems.put(result, msg);
            return result;
        }
    }

    /**
     * 删除一条消息，要删除的消息必须是被当前对象所取出来的
     * 
     * @param dataRow
     * @throws ClientException
     */
    public void delete(DataRow dataRow) throws Exception {
        if (rmqQueue == null) {
            if (!items.containsKey(dataRow))
                throw new RuntimeException("dataRow not find!");
            var message = items.get(dataRow);
            if (message != null) {
                getQueue().deleteMessage(message.getReceiptHandle());
                items.remove(dataRow);
            }
        } else {
            if (!rmqItems.containsKey(dataRow))
                throw new RuntimeException("dataRow not find!");
            var message = rmqItems.get(dataRow);
            if (message != null) {
                rmqQueue.consumer().ack(message);
                rmqItems.remove(dataRow);
            }
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
        if (rmqQueue == null) {
            if (maximum <= 0)
                throw new RuntimeException("maximum 必须大于 0");
            DataSet dataSet = new DataSet();
            int total = 0;
            Message msg = this.popMessage();
            while (msg != null) {
                total++;
                var row = dataSet.append().current();
                row.setJson(getMessageBody(msg));
                items.put(row, msg);
                if (total == maximum)
                    break;
                msg = this.popMessage();
            }
            return dataSet;
        } else {
            if (maximum <= 0)
                throw new RuntimeException("maximum 必须大于 0");
            DataSet dataSet = new DataSet();
            int total = 0;
            var msg = rmqQueue.consumer().recevie();
            while (msg != null) {
                total++;
                var row = dataSet.append().current();
                row.setJson(StandardCharsets.UTF_8.decode(msg.getBody()).toString());
                rmqItems.put(row, msg);
                if (total == maximum)
                    break;
                msg = rmqQueue.consumer().recevie();
            }
            return dataSet;
        }
    }

}
