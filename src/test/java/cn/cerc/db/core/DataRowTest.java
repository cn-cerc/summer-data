package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class DataRowTest {

    @Test
    public void test_create() {
        DataRow item = new DataRow();
        assertEquals(item.state(), DataRowState.None);
    }

    @Test
    public void test_setState() {
        DataRow item = new DataRow();
        item.setState(DataRowState.Insert);
        assertEquals(item.state(), DataRowState.Insert);
    }

    @Test
    public void test_setField() {
        DataRow item = new DataRow();
        String field1 = "code";
        String value1 = "value";
        item.setValue(field1, value1);
        assertEquals(value1, item.getValue(field1));

        String field2 = "num";
        Double value2 = 1.12345678;
        item.setValue(field2, value2);
        assertEquals(value2, (double) item.getValue(field2), 0);
    }

    @Test
    public void test_setField_error1() {
        DataRow item = new DataRow();
        DataRow obj = new DataRow();
        item.setValue("object", obj);
        assertEquals(obj, item.getValue("object"));
    }

    @Test(expected = RuntimeException.class)
    public void test_setField_error2() {
        DataRow item = new DataRow();
        item.setValue(null, "value");
    }

    @Test
    public void test_getInt() {
        DataRow item = new DataRow();
        long value = System.currentTimeMillis();
        item.setValue("value", value);
        assertEquals(value, (long) item.getDouble("value"));

        item.setValue("value", "2");
        assertEquals(2, item.getInt("value"));

        DataRow row = new DataRow();
        row.setValue("num", Short.valueOf(Short.MAX_VALUE));
        assertEquals(row.getInt("num"), Short.MAX_VALUE);
        row.setValue("num", Float.valueOf(Integer.MAX_VALUE));
        assertEquals(row.getInt("num"), Integer.MAX_VALUE);
        row.setValue("num", Double.valueOf(Integer.MAX_VALUE));
        assertEquals(row.getInt("num"), Integer.MAX_VALUE);
        row.setValue("num", Long.valueOf(Integer.MAX_VALUE));
        assertEquals(row.getInt("num"), Integer.MAX_VALUE);
    }

    @Test(expected = ClassCastException.class)
    public void test_getInt_Float1() {
        DataRow row = new DataRow();
        row.setValue("num", Float.valueOf(21474836471f));
        assertTrue(row.getInt("num") > 0);
    }

    @Test(expected = ClassCastException.class)
    public void test_getInt_Float2() {
        DataRow row = new DataRow();
        row.setValue("num", 2.1f);
        assertTrue(row.getInt("num") > 0);
    }

    @Test(expected = ClassCastException.class)
    public void test_getInt_Float3() {
        DataRow row = new DataRow();
        row.setValue("num", 2.1d);
        assertTrue(row.getInt("num") > 0);
    }

    @Test
    public void test_getInteger() {
        DataRow item = new DataRow();
        item.setValue("type", true);
        assertEquals(1, item.getDouble("type"), 0);
    }

    @Test
    public void test_setField_delta() {
        DataRow row = new DataRow();
        row.setValue("Code_", "a");
        assertEquals(row.delta().size(), 0);

        Object val = null;

        row.setValue("Code_", val);
        assertEquals(row.delta().size(), 0);

        row.setState(DataRowState.Update);
        row.setValue("Code_", val);
        assertEquals(row.delta().size(), 0);

        row.setValue("Code_", "c");
        row.setValue("Code_", "d");
        assertEquals(row.delta().size(), 1);
        assertTrue(row.getOldField("Code_") == val);
    }

    @Test
    public void test_map() {
        DataRow rs = new DataRow();
        rs.setValue("A", "A001");
        rs.setValue("B", "B001");
        rs.setValue("C", "C001");
        int i = 0;
        for (@SuppressWarnings("unused")
        String key : rs.fields().names()) {
            i++;
        }
        assertEquals(i, rs.size());
    }

    @Test(expected = RuntimeException.class)
    public void test_getBoolean() {
        DataRow row = new DataRow();
        row.setValue("a", new Datetime());
        System.out.println(row.getBoolean("a"));
    }

    @Test
    public void test_getDouble() {
        DataRow row = new DataRow();
        row.setValue("a", 9.82575d);
        assertEquals(row.getDouble("a"), 9.82575d, 0);
        assertEquals(row.getDouble("a", -4), 9.8258, 0);
    }

    @Test
    public void testGetDouble() {
        DataRow row = new DataRow();
        row.setValue("a", 1193.75);
        row.setValue("b", 1162.89);
//        1193.75 + 1162.89 = 2356.64
        System.out.println(row.getDouble("a") + row.getDouble("b"));// 2356.6400000000003
    }

    @Test
    public void test_of_1() {
        DataRow dataRow = DataRow.of("a", "1", "b", 2);
        assertEquals("""
                {"a":"1","b":2}""", dataRow.json());
    }

    @Test(expected=RuntimeException.class)
    public void test_of_2() {
        DataRow dataRow = DataRow.of("a", "1", "b", 2, "c");
        assertNotEquals("""
                {"a":"1","b":"2"}""", dataRow.json());
    }

    @Test
    public void test_of_3() {
        DataRow dataRow = DataRow.of("a", "1", "b", null);
        assertEquals("""
                {"a":"1","b":""}""", dataRow.json());
    }

    @Test(expected=RuntimeException.class)
    public void test_of_4() {
        DataRow dataRow = DataRow.of("a", "1", "", null);
        assertEquals("""
                {"a":"1","":""}""", dataRow.json());
    }

    @Test
    public void test_of_5() {
        DataRow dataRow = DataRow.of();
        assertEquals("{}", dataRow.json());
    }

}
