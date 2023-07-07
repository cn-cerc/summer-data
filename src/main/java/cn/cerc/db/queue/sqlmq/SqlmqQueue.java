package cn.cerc.db.queue.sqlmq;

import java.net.InetAddress;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.Datetime.DateType;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;
import cn.cerc.db.mysql.MysqlQuery;
import cn.cerc.db.queue.AbstractQueue;
import cn.cerc.db.queue.OnStringMessage;
import cn.cerc.db.queue.QueueServiceEnum;
import cn.cerc.db.redis.JedisFactory;
import cn.cerc.db.redis.Redis;
import redis.clients.jedis.Jedis;

public class SqlmqQueue implements IHandle {
    private static final Logger log = LoggerFactory.getLogger(SqlmqQueue.class);
    public static final String s_sqlmq_info = "s_sqlmq_info";
    public static final String s_sqlmq_log = "s_sqlmq_log";

    private String queue;
    private int delayTime = 0;
    private Optional<Datetime> showTime = Optional.empty();
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

    /**
     * 该方法用于设置顺序型消息队列序列1的消息总数
     * 
     * @param groupCode 消息分组
     * @param total     消息总数
     */
    public static void setGroupFirstTotal(String groupCode, int total) {
        try (Redis redis = new Redis()) {
            redis.setex(groupCode + 1, TimeUnit.DAYS.toSeconds(29), String.valueOf(total));
        }
    }

    public SqlmqQueue() {
        this.session = SqlmqServer.get().getSession();
    }

    public SqlmqQueue(String queue) {
        this.session = SqlmqServer.get().getSession();
        this.queue = queue;
    }

    public void pop(int maximum, OnStringMessage onConsume) {
        Datetime now = new Datetime();
        MysqlQuery query = new MysqlQuery(this);
        query.add("select * from %s", s_sqlmq_info);
        query.add("where (status_=%d or status_=%d or status_=%d)", StatusEnum.Waiting.ordinal(),
                StatusEnum.Next.ordinal(), StatusEnum.Working.ordinal());
        query.add("and show_time_ <= '%s'", now);
        query.add("and service_=%s", QueueServiceEnum.Sqlmq.ordinal());
        query.add("and queue_='%s'", this.queue);
        // FIXME 载入笔数需处理
        query.setMaximum(1);
        query.open();
        try (Redis redis = new Redis()) {
            for (var row : query) {
                if (onConsume instanceof AbstractQueue queue)
                    queue.setGroupCode(row.getString("group_code_"));
                consumeMessage(query, redis, row, onConsume);
            }
        }
    }

    public void consumeMessage(MysqlQuery query, Redis redis, DataRow row, OnStringMessage onConsume) {
        var uid = row.bind("UID_");
        var lockKey = "sqlmq." + uid.getString();
        if (redis.setnx(lockKey, new Datetime().toString()) == 0)
            return;
        int delayTime = query.getInt("delayTime_");
        redis.expire(lockKey, delayTime + 5);
        try {
            String content = "";
            boolean result = false;
            var groupCode = query.getString("group_code_");
            var currSequence = query.getInt("execution_sequence_");
            if (!Utils.isEmpty(groupCode) && currSequence == 1)
                SqlmqGroup.startExecute(groupCode);
            try {
                addLog(uid.getLong(), AckEnum.Read, content);
                query.edit();
                query.setValue("status_", StatusEnum.Working.ordinal());
                query.setValue("show_time_", new Datetime().inc(DateType.Second, delayTime));
                query.setValue("consume_times_", query.getInt("consume_times_") + 1);
                query.setValue("update_time_", new Datetime());
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
                if (!Utils.isEmpty(groupCode))
                    SqlmqGroup.incrDoneNum(groupCode);
            } else {
                query.edit();
                query.setValue("status_", StatusEnum.Next.ordinal());
                query.setValue("show_time_", new Datetime().inc(DateType.Second, delayTime));
            }
            query.setValue("update_time_", new Datetime());
            query.setValue("version_", query.getInt("version_") + 1);
            query.post();
            if (!result)
                return;

            if (Utils.isEmpty(groupCode) || currSequence < 1)
                return;

            // 启动Group消息处理
            int nextSequence = currSequence + 1;
            // 检查同级消息是否执行完，未执行完提前返回
            String msgTotalkey = groupCode + currSequence;
            if (redis.client().decr(msgTotalkey) > 0) {
                redis.expire(msgTotalkey, TimeUnit.DAYS.toSeconds(29));
                return;
            }
            redis.del(msgTotalkey);
            MysqlQuery queryNext = new MysqlQuery(query);
            queryNext.add("select * from %s", s_sqlmq_info);
            queryNext.add("where group_code_='%s'", groupCode);
            queryNext.add("and execution_sequence_=%s", nextSequence);
            queryNext.open();
            if (queryNext.eof())
                SqlmqGroup.stopExecute(groupCode);
            if (queryNext.size() > 1)
                redis.setex(groupCode + nextSequence, TimeUnit.DAYS.toSeconds(29), String.valueOf(queryNext.size()));
            while (queryNext.fetch()) {
                queryNext.edit();
                queryNext.setValue("show_time_", new Datetime());
                queryNext.post();
            }
        } finally {
            redis.del(lockKey);
        }
    }

    public String push(String message, String order) {
        return push(message, order, "", 0);
    }

    public String push(String message, String order, String groupCode, int executionSequence) {
        MysqlQuery query = new MysqlQuery(this);
        query.add("select * from %s", s_sqlmq_info);
        if (!Utils.isEmpty(groupCode) && executionSequence == 1) {
            query.addWhere().eq("group_code_", groupCode).eq("execution_sequence_", executionSequence).build();
            query.setMaximum(1);
            query.open();
            if (!query.eof()) {
                try (Jedis redis = JedisFactory.getJedis()) {
                    if (!redis.exists(groupCode + 1))
                        throw new RuntimeException("同一个消息分组中序列1存在多条消息！请先使用 setGroupFirstTotal 方法设置序列1的消息总数");
                }
            }
        } else {
            query.setMaximum(0);
            query.open();
        }

        query.append();
        query.setValue("queue_", this.queue);
        query.setValue("order_", order);
        query.setValue("show_time_", showTime.orElseGet(Datetime::new));
        query.setValue("message_", message);
        query.setValue("consume_times_", 0);
        query.setValue("group_code_", groupCode);
        query.setValue("execution_sequence_", executionSequence);
        query.setValue("status_", StatusEnum.Waiting.ordinal());

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

    public Optional<Datetime> getShowTime() {
        return showTime;
    }

    public void setShowTime(Datetime showTime) {
        this.showTime = Optional.ofNullable(showTime);
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
