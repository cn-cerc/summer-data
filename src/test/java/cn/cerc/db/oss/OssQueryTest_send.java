package cn.cerc.db.oss;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.StubDatabaseSession;

public class OssQueryTest_send implements IHandle {
    private OssQuery ds;
    private ISession session;

    @Before
    public void setUp() {
        session = new StubDatabaseSession();
        ds = new OssQuery(this);
    }

    /**
     * 保存文件/覆盖文件
     */
    @Test
    @Ignore
    public void saveFile() {
        ds.setOssMode(OssMode.create);
        ds.add("select * from %s", "id_00001.txt");
        ds.setOssMode(OssMode.readWrite);
        ds.open();
        ds.append();
        ds.setValue("num", ds.getInt("num") + 1);
        ds.save();
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
