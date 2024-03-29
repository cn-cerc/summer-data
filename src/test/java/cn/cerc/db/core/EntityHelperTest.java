package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class EntityHelperTest {

    @Test
    public void test() {
        var helper1 = EntityHelper.get(StubEntity.class);
        assertEquals(1, helper1.fields().size());
        assertEquals("test", helper1.tableName());
        assertEquals("Pgsql", helper1.sqlServerType().name());
        assertEquals("基类", helper1.description());

        var helper2 = EntityHelper.get(StubChildEntity.class);
        assertEquals(2, helper2.fields().size());
        assertEquals("test", helper2.tableName());
        assertEquals("Mysql", helper2.sqlServerType().name());
        assertEquals("子类", helper2.description());
    }

}
