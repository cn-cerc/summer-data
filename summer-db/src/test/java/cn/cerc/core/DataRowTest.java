package cn.cerc.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class DataRowTest {

    @Test
    public void test_create() {
        DataRow item = new DataRow();
        assertEquals(item.getState(), RecordState.dsNone);
    }

    @Test
    public void test_setState() {
        DataRow item = new DataRow();
        item.setState(RecordState.dsInsert);
        assertEquals(item.getState(), RecordState.dsInsert);
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
        DataRow rs = new DataRow();
        rs.setValue("Code_", "a");
        assertEquals(rs.getDelta().size(), 0);

        Object val = null;

        rs.setValue("Code_", val);
        assertEquals(rs.getDelta().size(), 0);

        rs.setState(RecordState.dsEdit);
        rs.setValue("Code_", val);
        assertEquals(rs.getDelta().size(), 0);

        rs.setValue("Code_", "c");
        rs.setValue("Code_", "d");
        assertEquals(rs.getDelta().size(), 1);
        assertTrue(rs.getOldField("Code_") == val);
    }

    @Test
    public void test_map() {
        DataRow rs = new DataRow();
        rs.setValue("A", "A001");
        rs.setValue("B", "B001");
        rs.setValue("C", "C001");
        int i = 0;
        for (@SuppressWarnings("unused")
        String key : rs.getItems().keySet()) {
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

}
