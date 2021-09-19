package cn.cerc.db.queue;

import cn.cerc.core.ClassResource;
import cn.cerc.core.DataRow;
import cn.cerc.db.SummerDB;
import cn.cerc.db.core.NosqlOperator;

public class QueueOperator implements NosqlOperator {
    private static final ClassResource res = new ClassResource(QueueOperator.class, SummerDB.ID);

    @Override
    public boolean insert(DataRow record) {
        throw new RuntimeException(res.getString(1, "消息队列服务，不支持插入操作"));
    }

    @Override
    public boolean update(DataRow record) {
        throw new RuntimeException(res.getString(2, "消息队列服务，不支持修改操作"));
    }

    @Override
    public boolean delete(DataRow record) {
        throw new RuntimeException(res.getString(3, "消息队列服务，不支持删除操作"));
    }
}
