package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.junit.Test;

import com.google.gson.Gson;

public class DataTypeTest {

    @Test
    public void test_setType_1() {
        assertEquals("b", new DataType().setClass(Boolean.class).toString());
        assertEquals("n1", new DataType().setClass(Integer.class).toString());
        assertEquals("n2", new DataType().setClass(Long.class).toString());
        assertEquals("f1", new DataType().setClass(Float.class).toString());
        assertEquals("f2", new DataType().setClass(Double.class).toString());
        assertEquals("s", new DataType().setClass(String.class).toString());
        //
        assertEquals("t", new DataType().setClass(FastTime.class).toString());
        assertEquals("d", new DataType().setClass(FastDate.class).toString());
        assertEquals("dt", new DataType().setClass(Datetime.class).toString());
        assertEquals("dt", new DataType().setClass(Date.class).toString());
    }

    @Test
    public void test_setType_2() {
        assertEquals("b", new DataType().setValue("b").toString());
        assertEquals("n1", new DataType().setValue("n1").toString());
        assertEquals("n2", new DataType().setValue("n2").toString());
        assertEquals("f1", new DataType().setValue("f1").toString());
        assertEquals("f2", new DataType().setValue("f2").toString());
        assertEquals("f2,4", new DataType().setValue("f2,4").toString());
        assertEquals("s", new DataType().setValue("s").toString());
        assertEquals("s", new DataType().setValue("s0").toString());
        assertEquals("s5", new DataType().setValue("s5").toString());
        //
        assertEquals("t", new DataType().setValue("t").toString());
        assertEquals("d", new DataType().setValue("d").toString());
        assertEquals("dt", new DataType().setValue("dt").toString());
    }

    @Test
    public void test_put() {
        DataType fieldType = new DataType();
        assertEquals("b", fieldType.readData(true).toString());
        assertEquals("b", fieldType.readData(false).toString());

        fieldType = new DataType();
        assertEquals("n1", fieldType.readData(1).toString());
        assertEquals("n1", fieldType.readData(9).toString());
        assertEquals("n2", fieldType.readData(1l).toString());
        assertEquals("n2", fieldType.readData(9l).toString());

        fieldType = new DataType();
        assertEquals("f1", fieldType.readData(1f).toString());
        assertEquals("f2", fieldType.readData(9d).toString());
        assertEquals("f2,4", fieldType.readData(9d).setDecimal(4).toString());

        fieldType = new DataType();
        assertEquals("s", fieldType.readData("").toString());
        assertEquals("s2", fieldType.readData("ab").toString());
        assertEquals("s4", fieldType.readData("abcd").toString());
        assertEquals("s4", fieldType.readData("abc").toString());
    }

    @Test
    public void test_clone() {
        DataType type1 = new DataType();
        type1.setClass(String.class);
        type1.setDecimal(1);
        type1.setLength(2);
        DataType type2 = type1.clone();
        Gson gson = new Gson();
        assertEquals(gson.toJson(type1), gson.toJson(type2));
    }
}
