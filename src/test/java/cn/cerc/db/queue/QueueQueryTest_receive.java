package cn.cerc.db.queue;

import org.junit.Before;
import org.junit.Test;

import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.StubSession;

public class QueueQueryTest_receive implements IHandle {
    private QueueQuery query;
    private ISession session;

    @Before
    public void setUp() {
        session = new StubSession();
        query = new QueueQuery("test");
    }

    @Test
    public void test() {
        System.out.println(query.open().json());
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
