package cn.cerc.db.mongo;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.StubDatabaseSession;

public class MongoQueryTest implements IHandle {
    private ISession session;
    private MongoQuery ds;

    @Before
    public void setUp() throws Exception {
        session = new StubDatabaseSession();
        ds = new MongoQuery(this);
    }

    @Test
    @Ignore
    public void test_open() {
        ds.add("select * from tmp2");
        ds.open();
        System.out.println(ds);
        while (ds.fetch()) {
            // 输出子数据表
            DataSet child = ds.getChildDataSet("data");
            System.out.println(child);
        }
    }

    @Test
    @Ignore
    public void test_append() {
        DataSet data = new DataSet();
        data.append();
        data.setValue("it", 1);
        data.setValue("month", "201609");
        data.append();
        data.setValue("it", 2);
        data.setValue("month", "201610");
        //
        ds.add("select * from tmp2 where code='a001'");
        ds.open();
        for (int i = 1; i < 20; i++) {
            ds.append();
            ds.setValue("code", "a001");
            ds.setValue("value", i);
            ds.post();
        }
        ds.append();
        ds.setValue("code", "a001QQ");
        ds.setValue("value", 30);
        ds.post();
    }

    @Test
    @Ignore
    public void test_modify() {
        ds.add("select * from tmp2");
        ds.open();
        while (ds.fetch()) {
            ds.edit();
            ds.setValue("value", ds.getInt("value") + 1);
            ds.post();
        }
    }

    @Test
    @Ignore
    public void test_select() {
        ds.add("select * from tmp2");
        ds.add("where value<>3 and value<5");
        ds.open();
        System.out.println(ds);
    }

    @Test
    @Ignore
    public void test_select2() {
        ds.add("select * from tmp2");
        ds.add("where value<>3 and value<5 and code=a001");
        ds.open();
        System.out.println(ds);
    }

    @Test
    @Ignore
    public void test_select3() {
        ds.add("select * from tmp2");
        ds.add("where code like 'QQ'");
        ds.open();
        System.out.println(ds);
    }

    @Test
    @Ignore
    public void test_delete() {
        ds.add("select * from tmp2 where code='a001' and value=3");
        ds.open();
        System.out.println(ds);
        while (!ds.eof())
            ds.delete();
    }

    @Override
    public ISession getSession() {
        return session;
    }

    @Override
    public void setSession(ISession session) {
        this.session = session;
    }
}
