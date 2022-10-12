package cn.cerc.db.queue;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.model.Message;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.DataSet;

public abstract class AbstractQueue {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);
    private CloudQueue cloudQueue;
    private Map<DataRow, Message> items = new HashMap<>();

    public abstract String getQueueId();

    /**
     * 将dataRow发送到当前队列
     * 
     * @param dataRow
     */
    public void write(DataRow dataRow) {
        Message message = new Message();
        message.setMessageBody(dataRow.json());
        var msg = queue().putMessage(message);
        items.put(dataRow, msg);
    }

    /**
     * 取出一条消息，类型为 DataRow，若没有则返回为null
     * 
     * @return DataRow
     */
    public DataRow read() {
        Message msg = this.popMessage();
        if (msg == null)
            return null;
        DataRow result = new DataRow();
        result.setJson(getMessageBody(msg));
        items.put(result, msg);
        return result;
    }

    protected String getMessageBody(Message msg) {
        return msg.getMessageBody();
    }

    /**
     * 一批取出多条消息，可通过dataSet.size()来判断实际取出了多少笔
     * 
     * @param maximum 最多取出多少笔，必须大于0
     * @return DataSet
     */
    public DataSet readBatch(int maximum) {
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
            queue().deleteMessage(message.getReceiptHandle());
    }

    private Message popMessage() {
        Message message = null;
        try {
            message = queue().popMessage();
            if (message != null) {
                log.debug("messageBody：{}", message.getMessageBodyAsString());
                log.debug("messageId：{}", message.getMessageId());
                log.debug("receiptHandle：{}", message.getReceiptHandle());
                log.debug(message.getMessageBody());
                return message;
            }
        } catch (ClientException e) {
            if (e.getMessage().indexOf("返回结果无效，无法解析。") > -1)
                return null;
            System.out.println("执行异常：" + e.getMessage());
        }
        return null;
    }

    private CloudQueue queue() {
        if (this.cloudQueue == null)
            this.cloudQueue = new QueueServer().openQueue(getQueueId());
        return cloudQueue;
    }

}
