package cn.cerc.db.queue;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class QueueGroupTest {

    @Test
    public void test() {
        var group = new QueueGroup("abc");
        assertEquals("abc", group.code());
        // 默认值: row=1, column = 0;
        assertEquals(1, group.row());
        assertEquals(0, group.column());
        // 增加1列
        assertEquals(1, group.incrColumn());
        assertEquals(1, group.column());
        // 增加1行
        assertEquals(2, group.incrRow());
        assertEquals(2, group.row());
    }

}
