package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DataFieldTest {

    @Test
    public void test() {
        DataRow row = DataRow.of("code", "001");

        DataField code = row.bind("code");
        assertEquals(1, code.getInt());

        // 连动更新
        code.setData("002");
        assertEquals("002", row.getString("code"));

        row.setValue("code", "003");
        assertEquals("003", code.getString());
        assertEquals(3, code.getInt());
    }

}
