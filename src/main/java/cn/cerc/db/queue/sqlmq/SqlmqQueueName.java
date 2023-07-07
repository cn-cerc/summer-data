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
import cn.cerc.db.mysql.MysqlQuery;
import cn.cerc.db.queue.AbstractQueue;

public class SqlmqQueueName {
    private static final Logger log = LoggerFactory.getLogger(SqlmqQueueName.class);

    public static final String TABLE = "s_sqlmq_queue_name";

    public static void register(Class<? extends AbstractQueue> queueClazz) {
        String queueName = Optional.ofNullable(queueClazz.getAnnotation(Description.class))
                .map(Description::value)
                .get();
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
        query.setValue("create_time_", new Datetime());
        query.post();
    }

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

}
