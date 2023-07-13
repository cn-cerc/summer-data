package cn.cerc.db.queue.sqlmq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Description;

import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.Utils;
import cn.cerc.db.dao.BatchScript;
import cn.cerc.db.mysql.MysqlQuery;
import cn.cerc.db.queue.AbstractQueue;

public class SqlmqQueueName {
    private static final Logger log = LoggerFactory.getLogger(SqlmqQueueName.class);

    public static final String TABLE = "s_sqlmq_queue_name";

    /**
     * 注册消费队列
     * 
     * @param queueClazz 队列Class
     */
    public static void register(Class<? extends AbstractQueue> queueClazz) {
        String queueName = null;
        Description annotaion = queueClazz.getAnnotation(Description.class);
        if (annotaion != null)
            queueName = annotaion.value();

        if (Utils.isEmpty(queueName)) {
            log.warn("{} 缺少 Description 注解，请相关人员填上队列描述", queueClazz);
            return;
        }
        String queueClass = queueClazz.getSimpleName();

        MysqlQuery query = new MysqlQuery(SqlmqServer.get());
        query.add("select * from %s", TABLE);
        query.addWhere().eq("queue_class_", queueClass).build();
        query.open();
        if (!query.eof())
            return;
        query.append();
        query.setValue("queue_class_", queueClass);
        query.setValue("queue_name_", queueName);
        query.setValue("consumer_times_", 0);
        query.setValue("consumer_count_", 0);
        query.setValue("create_time_", new Datetime());
        query.post();
    }

    /**
     * 获取队列名称
     * 
     * @param queueClass 队列Class
     * @return 队列名称
     */
    public static Optional<String> getQueueName(String queueClass) {
        MysqlQuery query = new MysqlQuery(SqlmqServer.get());
        query.add("select queue_name_ from %s", TABLE);
        query.addWhere().eq("queue_class_", queueClass).build();
        query.openReadonly();
        if (query.eof())
            return Optional.empty();
        else
            return Optional.of(query.getString("queue_name_"));
    }

    /**
     * 获取队列名称
     * 
     * @param queueClass 队列Class
     * @return 队列名称
     */
    public static Map<String, String> getQueueName(List<String> queueClass) {
        if (Utils.isEmpty(queueClass))
            return new HashMap<>();
        MysqlQuery query = new MysqlQuery(SqlmqServer.get());
        query.add("select queue_class_,queue_name_ from %s", TABLE);
        query.addWhere().in("queue_class_", queueClass).build();
        query.openReadonly();
        if (query.eof())
            return new HashMap<>();
        return query.toMap("queue_class_", "queue_name_");
    }

    /**
     * 更新消费耗时以及消费次数
     * 
     * @param queueClass 队列Class
     * @param usedTime   消费耗时
     */
    public static void updateConsumerTime(String queueClass, long usedTime) {
        BatchScript script = new BatchScript(SqlmqServer.get());
        script.add("update %s set consumer_times_=consumer_times_+%s,consumer_count_=consumer_count_+1", TABLE,
                usedTime);
        script.add("where queue_class_='%s'", queueClass);
        script.exec();
    }

    /**
     * 获取队列平均消费耗时
     * 
     * @param queueClass 队列Class
     * @return 平均耗时
     */
    public static long getQueueAvgUsedTime(String queueClass) {
        MysqlQuery query = new MysqlQuery(SqlmqServer.get());
        query.add("select consumer_times_,consumer_count_ from %s", TABLE);
        query.addWhere().eq("queue_class_", queueClass).build();
        query.openReadonly();
        if (query.getInt("consumer_count_") == 0)
            return 0;
        return query.getLong("consumer_times_") / query.getInt("consumer_count_");
    }

}
