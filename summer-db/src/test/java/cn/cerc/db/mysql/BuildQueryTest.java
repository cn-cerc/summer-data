package cn.cerc.db.mysql;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import cn.cerc.db.core.StubSession;

public class BuildQueryTest {

    private BuildQuery bs;
    private StubSession handle;

    @Before
    public void setUp() {
        handle = new StubSession();
        bs = new BuildQuery(handle);
    }

    @Test
    public void test_close() {
        bs.add("x");
        bs.byParam("x");
        bs.byField("x", "y");
        bs.setOrder("ok");
        bs.clear();
        assertEquals("", bs.sqlText());
    }

    @Test
    public void test_add() {
        bs.setMaximum(-1);
        bs.add("select * from %s", "TABLE");
        assertEquals("select * from TABLE", bs.sqlText());
        bs.setMaximum(-1);
        bs.add("where code='%s'", "X");
        assertEquals("select * from TABLE" + BuildQuery.vbCrLf + "where code='X'", bs.sqlText());
        bs.clear();
        assertEquals("", bs.sqlText());
    }

    @Test
    public void test_byField() {
        String obj = null;
        bs.byField("code", obj);
        assertEquals("", bs.select());
        bs.clear();

        bs.byField("code", "x");
        assertEquals("where code='x'", bs.select());
        bs.clear();

        bs.byField("code", "x*");
        assertEquals("where code like 'x%'", bs.select());
        bs.byField("name", "y");
        assertEquals("where code like 'x%' and name='y'", bs.select());
        bs.clear();

        bs.byField("code", "``");
        assertEquals("where code='`'", bs.select());
        bs.clear();

        bs.byField("code", "`is null");
        assertEquals("where (code is null or code='')", bs.select());
        bs.clear();

        bs.byField("code", "`=100");
        assertEquals("where code=100", bs.select());
        bs.clear();

        bs.byField("code", "`!=100");
        assertEquals("where code<>100", bs.select());
        bs.clear();

        bs.byField("code", "`<>100");
        assertEquals("where code<>100", bs.select());
        bs.clear();
    }
}
