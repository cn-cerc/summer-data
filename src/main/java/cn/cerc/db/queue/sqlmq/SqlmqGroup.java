package cn.cerc.db.queue.sqlmq;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.SqlWhere;
import cn.cerc.db.core.Utils;
import cn.cerc.db.mysql.MysqlQuery;
import cn.cerc.db.redis.Locker;

public class SqlmqGroup {

    private static final Logger log = LoggerFactory.getLogger(SqlmqGroup.class);

    public static final String TABLE = "s_sqlmq_group";

    public static final String LOCK_KEY = SqlmqGroup.class.getSimpleName();

    public static Optional<String> getGroupCode(IHandle handle, String project, String subItem) {
        MysqlQuery query = new MysqlQuery(SqlmqServer.get());
        query.add("select * from %s", TABLE);
        query.addWhere().eq("project_", project).eq("sub_item_", subItem).build();
        query.open();
        if (query.eof()) {
            String groupCode = UUID.randomUUID().toString();
            query.append();
            query.setValue("group_code_", groupCode);
            query.setValue("project_", project);
            query.setValue("sub_item_", subItem);
            query.setValue("total_", 1);
            query.setValue("done_num_", -1);
            query.setValue("create_user_", handle.getUserCode());
            query.setValue("create_time_", new Datetime());
            query.setValue("create_corp_", handle.getCorpNo());
            query.setValue("version_", 0);
            query.post();
            return Optional.of(groupCode);
        } else {
            return Optional.empty();
        }
    }

    public static void updateGroupCode(String groupCode, int total) {
        try (var locker = new Locker(groupCode, LOCK_KEY)) {
            if (!locker.lock("updateGroupCode", 1000 * 3))
                throw new RuntimeException(String.format("group: %s is locked", groupCode));
            MysqlQuery query = new MysqlQuery(SqlmqServer.get());
            query.add("select * from %s", TABLE);
            query.addWhere().eq("group_code_", groupCode).build();
            query.open();
            if (query.eof())
                throw new RuntimeException("not find message group: " + groupCode);

            if (query.getInt("total_") != total) {
                query.edit();
                query.setValue("total_", total);
                query.setValue("version_", query.getInt("version_") + 1);
                query.post();
            }
        }
    }

    public static Optional<MessageGroupRecord> getLastGroupCode(String project, String subItem) {
        MysqlQuery query = new MysqlQuery(SqlmqServer.get());
        query.add("select group_code_,total_,done_num_ from %s", TABLE);
        SqlWhere where = query.addWhere();
        where.eq("project_", project);
        if (!Utils.isEmpty(subItem))
            where.eq("sub_item_", subItem);
        where.build();
        query.add("order by UID_ desc");
        query.setMaximum(1);
        query.openReadonly();
        if (!query.eof()) {
            String groupCode = query.getString("group_code_");
            int total = query.getInt("total_");
            int doneNum = query.getInt("done_num_");
            if (doneNum < 0)
                doneNum = 0;
            boolean doneStatus = total == doneNum;
            return Optional.of(new MessageGroupRecord(groupCode, doneStatus, total, doneNum));
        }
        return Optional.empty();
    }

    public static void startExecute(String groupCode) {
        try (var locker = new Locker(groupCode, LOCK_KEY)) {
            if (!locker.lock("startExecute", 1000 * 3))
                throw new RuntimeException(String.format("group: %s is locked", groupCode));

            MysqlQuery query = new MysqlQuery(SqlmqServer.get());
            query.add("select * from %s", TABLE);
            query.addWhere().eq("group_code_", groupCode).build();
            query.open();
            if (query.eof()) {
                log.warn("未查询到 {} 消息组", groupCode);
                return;
            }
            int total = query.getInt("total_");
            int done = query.getInt("done_num_");
            if (done == total)
                throw new RuntimeException("子项目完成总数过多");

            if (done == -1) {
                query.edit();
                query.setValue("start_time_", new Datetime());
                query.setValue("done_num_", 0);
                query.setValue("start_num_", 1);
                query.setValue("version_", query.getInt("version_") + 1);
                query.post();
            } else {
                query.edit();
                query.setValue("start_num_", query.getInt("start_num_") + 1);
                query.setValue("version_", query.getInt("version_") + 1);
                query.post();
            }
        }
    }

    public static void stopExecute(String groupCode) {
        try (var locker = new Locker(groupCode, LOCK_KEY)) {
            if (!locker.lock("startExecute", 1000 * 3))
                throw new RuntimeException(String.format("group: %s is locked", groupCode));

            MysqlQuery query = new MysqlQuery(SqlmqServer.get());
            query.add("select * from %s", TABLE);
            query.addWhere().eq("group_code_", groupCode).build();
            query.open();
            if (query.eof()) {
                log.warn("未查询到 {} 消息组", groupCode);
                return;
            }

            int total = query.getInt("total_");
            int done = query.getInt("done_num_");
            if (done != total)
                throw new RuntimeException("子项目完成总数错误");

            query.edit();
            query.setValue("stop_time_", new Datetime());
            query.setValue("version_", query.getInt("version_") + 1);
            query.post();
        }
    }

    public static void incrDoneNum(String groupCode) {
        try (var locker = new Locker(groupCode, LOCK_KEY)) {
            if (!locker.lock("startExecute", 1000 * 3))
                throw new RuntimeException(String.format("group: %s is locked", groupCode));

            MysqlQuery query = new MysqlQuery(SqlmqServer.get());
            query.add("select * from %s", TABLE);
            query.addWhere().eq("group_code_", groupCode).build();
            query.open();
            if (query.eof()) {
                log.warn("未查询到 {} 消息组", groupCode);
                return;
            }
            int total = query.getInt("total_");
            int done = query.getInt("done_num_");
            if (done == total)
                throw new RuntimeException("子项目完成次数过多");
            if (done == -1)
                throw new RuntimeException("请先调用 startExecute 方法开始执行");

            query.edit();
            query.setValue("done_num_", done + 1);
            query.setValue("version_", query.getInt("version_") + 1);
            query.post();
        }
    }

    public static DataSet findGroupInfo(IHandle handle, String groupCode) {
        MysqlQuery query = new MysqlQuery(SqlmqServer.get());
        query.add("select * from %s", TABLE);
        query.addWhere().eq("group_code_", groupCode).build();
        query.openReadonly();
        if (query.eof()) {
            log.warn("未查询到 {} 消息组", groupCode);
            return new DataSet();
        }

        MysqlQuery queryQueue = new MysqlQuery(SqlmqServer.get());
        queryQueue.add("select * from %s", SqlmqQueue.s_sqlmq_info);
        queryQueue.addWhere().eq("group_code_", groupCode).build();
        queryQueue.openReadonly().disableStorage();

        // 获取队列名称
        List<String> queues = queryQueue.records()
                .stream()
                .map(row -> row.getString("queue_class_"))
                .distinct()
                .toList();
        Map<String, String> queueNameMap = SqlmqQueueName.getQueueName(queues);
        while (queryQueue.fetch()) {
            String queueClass = queryQueue.getString("queue_class_");
            queryQueue.setValue("queue_name_", queueNameMap.getOrDefault(queueClass, queueClass));
        }

        if (query.getInt("done_num_") < 0)
            query.setValue("done_num_", 0);
        queryQueue.head().copyValues(query.current());
        return queryQueue;
    }

}
