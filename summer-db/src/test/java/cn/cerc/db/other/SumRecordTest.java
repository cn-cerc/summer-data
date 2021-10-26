package cn.cerc.db.other;

import cn.cerc.db.SampleData;
import cn.cerc.db.core.DataSet;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SumRecordTest {
    private SumRecord sum;
    private DataSet ds;

    @Before
    public void setUp() throws Exception {
        ds = SampleData.getDataSet();
        sum = new SumRecord(ds);
    }

    @Test
    public void testRun() {
        sum.addField("num");
        sum.run();
        assertEquals(45, sum.getDouble("num"), 0);
    }

}
