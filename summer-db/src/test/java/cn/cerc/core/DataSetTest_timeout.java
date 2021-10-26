package cn.cerc.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DataSetTest_timeout {
    private DataSet ds = new DataSet();;
    private static final int MAX = 10000;

    @Test
    public void test_append() {
        ds.append();
        ds.setValue("code", "value");
        ds.post();
        assertEquals(1, ds.size());
    }

    @Before
    public void setUp() throws Exception {
        ds = new DataSet();
    }

    @After
    public void tearDown() throws Exception {
        ds.close();
    }

    @Test(timeout = 10000)
    public void testLocate_1_old() {
        for (int i = 0; i < MAX; i++) {
            String key = "code" + i;
            ds.append();
            ds.setValue("code", key);
            ds.setValue("value", i);
            ds.post();
        }
        for (int i = 100; i < MAX; i++)
            assertTrue("查找错误", ds.locate("value", i));
    }

    @Test(timeout = 50000)
    public void testLocate_2_new() {
        for (int i = 0; i < MAX; i++) {
            String key = "code" + i;
            ds.append();
            ds.setValue("code", key);
            ds.setValue("value", i);
            ds.post();
        }
        for (int i = 100; i < MAX; i++) {
            assertTrue(ds.lookup("value", i) != null);
        }
        ds.append();
        ds.setValue("code", "codexx");
        ds.setValue("value", "xx");
        ds.post();
        assertTrue(ds.lookup("value", "xx") != null);

        DataRow record = ds.lookup("value", "xx");
        record.setValue("code", "value");
        assertEquals(ds.getString("code"), "value");
        assertEquals(10001, ds.getRecNo());

        ds.setValue("code", "value2");
        assertEquals(record.getString("code"), "value2");

        assertTrue(ds.lookup("value", "xx") != null);
    }

}
