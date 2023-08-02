package cn.cerc.db.testsql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import cn.cerc.db.core.DataSet;

public class SqlWhereFilterTest {

    @Test
    public void test_1() {
        var obj = new SqlWhereFilter("where a=1 and b=2");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1);
        ds.append().setValue("a", 1).setValue("b", 2);
        ds.append().setValue("a", 2).setValue("b", 3);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b"],[1,2]]}""", ds.toString());
    }

    @Test
    public void test_2() {
        var obj = new SqlWhereFilter("where a=1");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1);
        ds.append().setValue("a", 1).setValue("b", 2);
        ds.append().setValue("a", 2).setValue("b", 3);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b"],[1,1],[1,2]]}""", ds.toString());
    }

}
