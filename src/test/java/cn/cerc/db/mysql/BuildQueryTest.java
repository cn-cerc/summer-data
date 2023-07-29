package cn.cerc.db.mysql;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import cn.cerc.db.core.Handle;
import cn.cerc.db.core.StubDatabaseSession;

public class BuildQueryTest {

    private BuildQuery bs;
    private StubDatabaseSession handle;

    @Before
    public void setUp() {
        handle = new StubDatabaseSession();
        bs = new BuildQuery(new Handle(handle));
    }

    @Test
    public void test_close() {
        bs.add("x");
        bs.byParam("x");
        bs.byField("x", "y");
        bs.setOrder("order by ok");
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
        assertEquals("", bs.sqlText());
        bs.clear();

        bs.byField("code", "x");
        assertEquals("where code='x'", bs.sqlText());
        bs.clear();

        bs.byField("code", "x*");
        assertEquals("where code like 'x%'", bs.sqlText());
        bs.byField("name", "y");
        assertEquals("where code like 'x%' and name='y'", bs.sqlText());
        bs.clear();

        bs.byField("code", "``");
        assertEquals("where code='`'", bs.sqlText());
        bs.clear();

        bs.byField("code", "`is null");
        assertEquals("where (code is null or code='')", bs.sqlText());
        bs.clear();

        bs.byField("code", "`=100");
        assertEquals("where code=100", bs.sqlText());
        bs.clear();

        bs.byField("code", "`!=100");
        assertEquals("where code<>100", bs.sqlText());
        bs.clear();

        bs.byField("code", "`<>100");
        assertEquals("where code<>100", bs.sqlText());
        bs.clear();
    }
}
