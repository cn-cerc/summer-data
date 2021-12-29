package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FieldDefsTest {
    private FieldDefs defs;

    @Before
    public void setUp() throws Exception {
        defs = new FieldDefs(EntityTest.class);
    }

    @After
    public void tearDown() throws Exception {
        defs.clear();
    }

    @Test
    public void test_id() {
        FieldMeta meta = defs.get("id_");
        assertTrue(meta.storage());
        assertTrue(meta.identification());
        assertTrue(meta.autoincrement());
        assertFalse(meta.insertable());
        assertFalse(meta.updatable());
        assertFalse(meta.nullable());
        assertEquals("n1", meta.typeValue());
    }

    @Test
    public void test_code() {
        FieldMeta meta = defs.get("code_");
        assertTrue(meta.storage());
        assertFalse(meta.identification());
        assertFalse(meta.autoincrement());
        assertTrue(meta.insertable());
        assertTrue(meta.updatable());
        assertFalse(meta.nullable());
        assertEquals("s30", meta.typeValue());
    }

    @Test
    public void test_name() {
        FieldMeta meta = defs.get("name_");
        assertTrue(meta.storage());
        assertFalse(meta.identification());
        assertFalse(meta.autoincrement());
        assertTrue(meta.insertable());
        assertTrue(meta.updatable());
        assertFalse(meta.nullable());
        assertEquals("s50", meta.typeValue());
    }

    @Test
    public void test_remark() {
        FieldMeta meta = defs.get("remark_");
        assertTrue(meta.storage());
        assertFalse(meta.identification());
        assertFalse(meta.autoincrement());
        assertTrue(meta.insertable());
        assertTrue(meta.updatable());
        assertTrue(meta.nullable());
        assertEquals("s100", meta.typeValue());
    }

    @Test
    public void test_version() {
        FieldMeta meta = defs.get("version_");
        assertTrue(meta.storage());
        assertFalse(meta.identification());
        assertFalse(meta.autoincrement());
        assertTrue(meta.insertable());
        assertTrue(meta.updatable());
        assertFalse(meta.nullable());
        assertEquals("n1", meta.typeValue());
    }

    public static void main(String[] args) {
        FieldDefs defs = new FieldDefs(EntityTest.class);
        System.out.println(defs);
    }
}
