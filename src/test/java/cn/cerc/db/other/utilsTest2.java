package cn.cerc.db.other;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import cn.cerc.db.core.Utils;

public class utilsTest2 {

    @Test
    public void testRoundTo() {
        assertEquals("" + Utils.roundTo(1.234, -2), "1.23");
        assertEquals("" + Utils.roundTo(1.235, -2), "1.24");
        assertEquals("" + Utils.roundTo(1.245, -2), "1.24");
        assertEquals("" + Utils.roundTo(1.2451, -2), "1.25");
    }
}
