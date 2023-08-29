package cn.cerc.db.queue;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class QueueGroupTest {

    @Test
    public void test() {
        try (var group = new QueueGroup("abc", 1)) {
            assertEquals("abc", group.code());
            // 默认值: row=1, column = 0;
            assertEquals(1, group.executionSequence());
            assertEquals(0, group.total());
            // 增加1列
            assertEquals(1, group.incr());
            assertEquals(1, group.total());
            // 增加1行
            assertEquals(2, group.next());
            assertEquals(2, group.executionSequence());
        }
    }

}
