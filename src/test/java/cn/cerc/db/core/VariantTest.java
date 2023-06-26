package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class VariantTest {

    @Test
    public void test() {
        Variant tmp = new Variant("value").setKey("key");
        assertEquals(tmp.key(), "key");
        
        assertEquals(tmp.getString(), "value");
    }

}
