package cn.cerc.db.queue;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.mns.model.Message;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.DataSet;

public abstract class AbstractDataRowQueue extends AbstractQueue {
    private transient Map<DataRow, Message> items = new HashMap<>();

    @Override
    public String getQueueId() {
        return this.getClass().getSimpleName();
    }

    /**
     * 将dataRow发送到当前队列
     * 
     * @param dataRow
     */
    public void append(DataRow dataRow) {
        Message message = new Message();
        message.setMessageBody(dataRow.json());
        getQueue().putMessage(message);
    }

    /**
     * 取出一条消息，类型为 DataRow，若没有则返回为null
     * 
     * @return DataRow
     */
    public DataRow receive() {
        Message msg = this.popMessage();
        if (msg == null)
            return null;
        DataRow result = new DataRow();
        result.setJson(getMessageBody(msg));
        items.put(result, msg);
        return result;
    }

    /**
     * 删除一条消息，要删除的消息必须是被当前对象所取出来的
     * 
     * @param dataRow
     */
    public void delete(DataRow dataRow) {
        if (!items.containsKey(dataRow))
            throw new RuntimeException("dataRow not find!");
        var message = items.get(dataRow);
        if (message != null)
            getQueue().deleteMessage(message.getReceiptHandle());
    }

    /**
     * 一批取出多条消息，可通过dataSet.size()来判断实际取出了多少笔
     * 
     * @param maximum 最多取出多少笔，必须大于0
     * @return DataSet
     */
    public DataSet receive(int maximum) {
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
    }

}
