package cn.cerc.db.queue;

import org.junit.Before;
import org.junit.Test;

import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.StubSession;

public class QueueQueryTest_append implements IHandle {
    private QueueQuery query;
    private ISession session;

    @Before
    public void setUp() {
        session = new StubSession();
        query = new QueueQuery("test");
    }

    @Test
    public void test() {
        DataSet dataSet = new DataSet();

        // append head
        dataSet.head().setValue("queueHeadData1", "queueHeadData1");
        dataSet.head().setValue("queueHeadData2", "queueHeadData2");
        dataSet.head().setValue("queueHeadData3", "queueHeadData3");
        dataSet.head().setValue("queueHeadData4", "queueHeadData4");

        // append body
        dataSet.append();
        dataSet.setValue("queueBodyData1", "queueBodyData1");
        dataSet.setValue("queueBodyData2", "queueBodyData2");
        dataSet.setValue("queueBodyData3", "queueBodyData3");
        dataSet.setValue("queueBodyData4", "queueBodyData4");
        dataSet.setValue("queueBodyData5", "queueBodyData5");

        query.save(dataSet.json());
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
