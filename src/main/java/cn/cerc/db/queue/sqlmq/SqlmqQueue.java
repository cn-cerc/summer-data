package cn.cerc.db.queue.sqlmq;

import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.Datetime.DateType;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.mysql.MysqlQuery;
import cn.cerc.db.queue.OnStringMessage;

public class SqlmqQueue {
    private static final String s_sqlmq_info = "s_sqlmq_info";
    private static final String s_sqlmq_log = "s_sqlmq_log";

    private IHandle handle;
    private String queue;

    public enum AckEnum {
        Read,
        Ok,
        Error;
    }

    public enum StatusEnum {
        Waiting,
        Working,
        Finish,
        Next;
    }

    public SqlmqQueue(String queue) {
        this.handle = SqlmqServer.get();
        this.queue = queue;
    }

    public void pop(int maximum, OnStringMessage onConsume) {
        MysqlQuery query = new MysqlQuery(handle);
        query.add("select * from %s", s_sqlmq_info);
        query.add("where queue_='%s'", this.queue);
        query.add("and ((status_=%d)", StatusEnum.Waiting.ordinal());
        query.add("or (status_=%d and show_time_ <= '%s'))", StatusEnum.Next.ordinal(), new Datetime());
        query.setMaximum(maximum);
        query.open();
        for (var row : query) {
            boolean result = false;
            String content = "";
            var uid = row.bind("UID_");
            try {
                addLog(uid.getLong(), AckEnum.Read, content);
                query.edit();
                query.setValue("status_", StatusEnum.Working.ordinal());
                query.setValue("consume_times_", query.getInt("consume_times_") + 1);
                query.setValue("version_", query.getInt("version_") + 1);
                query.post();
                result = onConsume.consume(row.getString("message_"));
            } catch (Exception e) {
                content = e.getMessage();
            }
            addLog(uid.getLong(), result ? AckEnum.Ok : AckEnum.Error, content);

            if (result) {
                query.edit();
                query.setValue("status_", StatusEnum.Finish.ordinal());
            } else {
                query.edit();
                query.setValue("status_", StatusEnum.Next.ordinal());
                query.setValue("show_time_", new Datetime().inc(DateType.Minute, 30));
            }
            query.setValue("version_", query.getInt("version_") + 1);
            query.post();
        }
    }

    public String push(String message, String order) {
        MysqlQuery query = new MysqlQuery(handle);
        query.add("select * from %s", s_sqlmq_info);
        query.setMaximum(0);
        query.open();

        query.append();
        query.setValue("queue_", this.queue);
        query.setValue("order_", order);
        query.setValue("show_time_", new Datetime());
        query.setValue("message_", message);
        query.setValue("consume_times_", 0);
        query.setValue("status_", StatusEnum.Waiting.ordinal());

        query.setValue("version_", 0);
        query.setValue("create_user_", handle.getUserCode());
        query.setValue("create_time_", new Datetime());
        query.setValue("update_time_", new Datetime());
        query.post();
        return query.getString("UID_");
    }

    private void addLog(long queueId, AckEnum ack, String content) {
        MysqlQuery query = new MysqlQuery(handle);
        query.add("select * from %s", s_sqlmq_log);
        query.setMaximum(0);
        query.open();

        query.append();
        query.setValue("queue_id_", queueId);
        query.setValue("ack_", ack.ordinal());
        query.setValue("content_", content);
        query.setValue("create_time_", new Datetime());
        query.post();
    }

}
