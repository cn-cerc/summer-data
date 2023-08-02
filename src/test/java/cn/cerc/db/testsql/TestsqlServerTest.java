package cn.cerc.db.testsql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import cn.cerc.db.core.Datetime;

public class TestsqlServerTest {

    @Test
    public void test() {
        var text = "2023-08-01 11:11:11";
        var temp = new Datetime(text).getTimestamp();
        assertEquals(text, new Datetime(temp).toString());
        var db = TestsqlServer.build();
        db.lockTime(temp);
        var dt = new Datetime();
        assertEquals(text, dt.toString());
    }

}
