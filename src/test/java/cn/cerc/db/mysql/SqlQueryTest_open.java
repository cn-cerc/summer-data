package cn.cerc.db.mysql;

import org.junit.Before;

import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.StubDatabaseSession;

public class SqlQueryTest_open implements IHandle {
    private MysqlQuery ds;
    private ISession session;

    @Before
    public void setUp() {
        session = new StubDatabaseSession();
        ds = new MysqlQuery(this);
        ds.sql().setMaximum(1);
        ds.add("select CorpNo_,CWCode_,PartCode_ from TranB1B where CorpNo_='%s'", "911001");
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
