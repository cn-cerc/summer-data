package cn.cerc.db.nas;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.StubSession;

public class NasQueryTest implements IHandle {
    private static final Logger log = LoggerFactory.getLogger(NasQueryTest.class);

    private NasQuery ds;
    private ISession session;

    @Before
    public void setUp() {
        session = new StubSession();
        ds = new NasQuery(this);
    }

    /**
     * 保存文件/覆盖文件
     *
     * @Description
     * @author rick_zhou
     */
    @Test
    public void saveFile() {
        /*
         * ds.add("select test.txt from %s", appdb.get(handle,appdb.NAS_FOLDER));
         */
        ds.add("select test.txt from %s", "D://testFolder1/testFolder2");
        ds.setNasMode(NasModel.create);
        ds.open();
        ds.append();
        ds.setValue("key", "一大串字符串................................................");
        ds.save();
        // update
        ds.setValue("key", "一大串字符串2................................................");
        ds.save();
        ds.setValue("key2", "一大串字符串3................................................");
        ds.save();
    }

    /**
     * 获取文件内容
     *
     * @Description
     * @author rick_zhou
     */
    @Test
    @Ignore
    public void findFile() {
        /*
         * ds.add("select test.txt from %s", appdb.get(handle,appdb.NAS_FOLDER));
         */
        ds.add("select test.txt from %s", "D://testFolder1/testFolder2");
        ds.setNasMode(NasModel.readWrite);
        ds.open();
        log.info("获取到的文件内容为:\n" + ds.getValue("key"));
        log.info("获取到的文件内容为:\n" + ds.getValue("key2"));
    }

    /**
     * 删除文件
     *
     * @Description
     * @author rick_zhou
     */
    @Test
    @Ignore
    public void deleteFile() {
        /*
         * ds.add("select test.txt from %s", appdb.get(handle,appdb.NAS_FOLDER));
         */
        ds.add("select test.txt from %s", "D://testFolder1/testFolder2");
        ds.open();
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
