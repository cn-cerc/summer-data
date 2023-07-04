package cn.cerc.db.queue.sqlmq;

import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.Datetime.DateType;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.mysql.MysqlQuery;
import cn.cerc.db.queue.OnStringMessage;
import cn.cerc.db.queue.QueueServiceEnum;
import cn.cerc.db.redis.Redis;

public class SqlmqQueue implements IHandle {
    private static final Logger log = LoggerFactory.getLogger(SqlmqQueue.class);
    public static final String s_sqlmq_info = "s_sqlmq_info";
    public static final String s_sqlmq_log = "s_sqlmq_log";

    private String queue;
    private int delayTime = 0;
    private int showTime = 0;
    private QueueServiceEnum service = QueueServiceEnum.Sqlmq;
    private ISession session;
    private String queueClass;

    public enum AckEnum {
        Read,
        Ok,
        Error;
    }

    public enum StatusEnum {
        Waiting,
        Working,
        Finish,
        Next,
        Invalid;
    }

    public SqlmqQueue() {
        this.session = SqlmqServer.get().getSession();
    }

    public SqlmqQueue(String queue) {
        this.session = SqlmqServer.get().getSession();
        this.queue = queue;
    }

    public void pop(int maximum, OnStringMessage onConsume) {
        MysqlQuery query = new MysqlQuery(this);
        query.add("select * from %s", s_sqlmq_info);
        query.add("where ((status_=%d", StatusEnum.Waiting.ordinal());
        query.add("or status_=%d) and show_time_ <= '%s')", StatusEnum.Next.ordinal(), new Datetime());
        query.add("and service_=%s", QueueServiceEnum.Sqlmq.ordinal());
        query.add("and queue_='%s'", this.queue);
        // FIXME 载入笔数需处理
        query.setMaximum(1);
        query.open();
        try (Redis redis = new Redis()) {
            for (var row : query) {
                consumeMessage(query, redis, row, onConsume);
            }
        }
    }

    public void consumeMessage(MysqlQuery query, Redis redis, DataRow row, OnStringMessage onConsume) {
        var uid = row.bind("UID_");
        var lockKey = "sqlmq." + uid.getString();
        if (redis.setnx(lockKey, new Datetime().toString()) == 0)
            return;
        redis.expire(lockKey, 60 * 30);
        try {
            String content = "";
            boolean result = false;
            try {
                addLog(uid.getLong(), AckEnum.Read, content);
                query.edit();
                query.setValue("status_", StatusEnum.Working.ordinal());
                query.setValue("consume_times_", query.getInt("consume_times_") + 1);
                query.setValue("version_", query.getInt("version_") + 1);
                query.post();
                result = onConsume.consume(row.getString("message_"), true);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                content = e.getMessage();
            }
            addLog(uid.getLong(), result ? AckEnum.Ok : AckEnum.Error, content);

            if (result) {
                query.edit();
                query.setValue("status_", StatusEnum.Finish.ordinal());
            } else {
                query.edit();
                query.setValue("status_", StatusEnum.Next.ordinal());
                query.setValue("show_time_", new Datetime().inc(DateType.Second, query.getInt("delayTime_")));
            }
            query.setValue("version_", query.getInt("version_") + 1);
            query.post();
        } finally {
            redis.del(lockKey);
        }
    }

    public String push(String message, String order) {
        MysqlQuery query = new MysqlQuery(this);
        query.add("select * from %s", s_sqlmq_info);
        query.setMaximum(0);
        query.open();

        query.append();
        query.setValue("queue_", this.queue);
        query.setValue("order_", order);
        query.setValue("show_time_", new Datetime().inc(DateType.Second, showTime));
        query.setValue("message_", message);
        query.setValue("consume_times_", 0);
        query.setValue("status_", StatusEnum.Waiting);

        query.setValue("delayTime_", delayTime);
        query.setValue("service_", service.ordinal());
        query.setValue("product_", ServerConfig.getAppProduct());
        query.setValue("industry_", ServerConfig.getAppOriginal());
        query.setValue("queue_class_", this.queueClass);
        query.setValue("version_", 0);
        query.setValue("create_user_", getUserCode());
        query.setValue("create_time_", new Datetime());
        query.setValue("update_time_", new Datetime());
        query.post();
        return query.getString("UID_");
    }

    private void addLog(long queueId, AckEnum ack, String content) {
        try {
            MysqlQuery query = new MysqlQuery(this);
            query.add("select * from %s", s_sqlmq_log);
            query.setMaximum(0);
            query.open();

            query.append();
            query.setValue("queue_id_", queueId);
            query.setValue("ack_", ack.ordinal());
            query.setValue("content_", content);
            query.setValue("create_time_", new Datetime());
            query.setValue("ip_", InetAddress.getLocalHost().getHostAddress());
            query.post();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public int getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }

    public int getShowTime() {
        return showTime;
    }

    public void setShowTime(int showTime) {
        this.showTime = showTime;
    }

    public void setService(QueueServiceEnum service) {
        this.service = service;
    }

    public QueueServiceEnum getService() {
        return service;
    }

    @Override
    public ISession getSession() {
        return this.session;
    }

    @Override
    public void setSession(ISession session) {
        this.session = session;
    }

    public String getQueueClass() {
        return queueClass;
    }

    public void setQueueClass(String queueClass) {
        this.queueClass = queueClass;
    }

}
