package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class SyncDataSetTest {
    private DataSet src = new DataSet();
    private DataSet tar = new DataSet();

    @Before
    public void setUp() throws Exception {
        src.append();
        src.setValue("code", "a");
        src.append();
        src.setValue("code", "b");
        src.append();
        src.setValue("code", "c");

        tar.append();
        tar.setValue("code", "a");
        tar.append();
        tar.setValue("code", "c");
        tar.append();
        tar.setValue("code", "d");
    }

    @Test
    public void test() throws ServiceException, DataException {
        SyncDataSet sds = new SyncDataSet(src, tar, "code");

        int total = sds.execute(new ISyncDataSet() {
            @Override
            public void process(DataRow src, DataRow tar) throws SyncUpdateException {
                if (tar == null)
                    assertEquals("insert record: b", "insert record: " + src.getValue("code"));
                else if (src == null)
                    assertEquals("delete record: d", "delete record: " + tar.getValue("code"));
                else {
                    String code = src.getString("code");
                    if ("a".equals(code))
                        assertEquals("update record: a", "update record: " + src.getValue("code"));
                    else
                        assertEquals("update record: c", "update record: " + src.getValue("code"));
                }
            }
        });
        assertEquals(total, 4);
    }

}
