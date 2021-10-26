package cn.cerc.core;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.junit.Test;

import com.google.gson.Gson;

import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.FastDate;
import cn.cerc.db.core.FastTime;
import cn.cerc.db.core.FieldType;

public class FieldTypeTest {

    @Test
    public void test_setType_1() {
        assertEquals("b", new FieldType().setType(Boolean.class).toString());
        assertEquals("n1", new FieldType().setType(Integer.class).toString());
        assertEquals("n2", new FieldType().setType(Long.class).toString());
        assertEquals("f1", new FieldType().setType(Float.class).toString());
        assertEquals("f2", new FieldType().setType(Double.class).toString());
        assertEquals("s", new FieldType().setType(String.class).toString());
        //
        assertEquals("t", new FieldType().setType(FastTime.class).toString());
        assertEquals("d", new FieldType().setType(FastDate.class).toString());
        assertEquals("dt", new FieldType().setType(Datetime.class).toString());
        assertEquals("dt", new FieldType().setType(Date.class).toString());
    }

    @Test
    public void test_setType_2() {
        assertEquals("b", new FieldType().setType("b").toString());
        assertEquals("n1", new FieldType().setType("n1").toString());
        assertEquals("n2", new FieldType().setType("n2").toString());
        assertEquals("f1", new FieldType().setType("f1").toString());
        assertEquals("f2", new FieldType().setType("f2").toString());
        assertEquals("f2,4", new FieldType().setType("f2,4").toString());
        assertEquals("s", new FieldType().setType("s").toString());
        assertEquals("s", new FieldType().setType("s0").toString());
        assertEquals("s5", new FieldType().setType("s5").toString());
        //
        assertEquals("t", new FieldType().setType("t").toString());
        assertEquals("d", new FieldType().setType("d").toString());
        assertEquals("dt", new FieldType().setType("dt").toString());
    }

    @Test
    public void test_put() {
        FieldType fieldType = new FieldType();
        assertEquals("b", fieldType.put(true).toString());
        assertEquals("b", fieldType.put(false).toString());

        fieldType = new FieldType();
        assertEquals("n1", fieldType.put(1).toString());
        assertEquals("n1", fieldType.put(9).toString());
        assertEquals("n2", fieldType.put(1l).toString());
        assertEquals("n2", fieldType.put(9l).toString());

        fieldType = new FieldType();
        assertEquals("f1", fieldType.put(1f).toString());
        assertEquals("f2", fieldType.put(9d).toString());
        assertEquals("f2,4", fieldType.put(9d).setDecimal(4).toString());

        fieldType = new FieldType();
        assertEquals("s", fieldType.put("").toString());
        assertEquals("s2", fieldType.put("ab").toString());
        assertEquals("s4", fieldType.put("abcd").toString());
        assertEquals("s4", fieldType.put("abc").toString());
    }
    
    @Test
    public void test_clone() {
        FieldType type1 = new FieldType();
        type1.setType(String.class);
        type1.setDecimal(1);
        type1.setLength(2);
        FieldType type2 = type1.clone();
        Gson gson = new Gson();
        assertEquals(gson.toJson(type1), gson.toJson(type2));
    }
}
