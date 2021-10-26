package cn.cerc.db.queue;

import org.junit.Before;
import org.junit.Test;

import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.StubSession;

public class QueueQueryTest_append implements IHandle {
    private QueueQuery dataSet;
    private ISession session;

    @Before
    public void setUp() {
        session = new StubSession();
        dataSet = new QueueQuery(this);
    }

    @Test
    public void test() {
        // 增加模式
        dataSet.setQueueMode(QueueMode.append);
        dataSet.add("select * from test");
        dataSet.open();
        System.out.println(dataSet.getActive());

        // append head
        dataSet.getHead().setValue("queueHeadData1", "queueHeadData1");
        dataSet.getHead().setValue("queueHeadData2", "queueHeadData2");
        dataSet.getHead().setValue("queueHeadData3", "queueHeadData3");
        dataSet.getHead().setValue("queueHeadData4", "queueHeadData4");

        // append body
        dataSet.append();
        dataSet.setValue("queueBodyData1", "queueBodyData1");
        dataSet.setValue("queueBodyData2", "queueBodyData2");
        dataSet.setValue("queueBodyData3", "queueBodyData3");
        dataSet.setValue("queueBodyData4", "queueBodyData4");
        dataSet.setValue("queueBodyData5", "queueBodyData5");

        dataSet.save();
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
