package cn.cerc.db.mysql;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.StubDatabaseSession;

public class SqlQueryTest_attach implements IHandle {
    private MysqlQuery ds;
    private ISession session;

    @Before
    public void setUp() {
        session = new StubDatabaseSession();
        ds = new MysqlQuery(this);
    }

    @Test
    @Ignore
    public void test() {
        String sql = "select * from ourinfo where CorpNo_='%s'";
        ds.attach(String.format(sql, "000000"));
        ds.attach(String.format(sql, "144001"));
        ds.attach(String.format(sql, "911001"));
        for (DataRow record : ds) {
            System.out.println(record.toString());
        }
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
