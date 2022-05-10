package cn.cerc.db.queue;

import cn.cerc.db.SummerDB;
import cn.cerc.db.core.ClassResource;
import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.NosqlOperator;

public class OssOperator implements NosqlOperator {
    private static final ClassResource res = new ClassResource(OssOperator.class, SummerDB.ID);

    @Override
    public boolean insert(DataRow record) {
        throw new RuntimeException(res.getString(1, "阿里云OSS服务，不支持插入操作"));
    }

    @Override
    public boolean update(DataRow record) {
        throw new RuntimeException(res.getString(2, "阿里云OSS服务，不支持修改操作"));
    }

    @Override
    public boolean delete(DataRow record) {
        throw new RuntimeException(res.getString(3, "阿里云OSS服务，不支持删除操作"));
    }
}
