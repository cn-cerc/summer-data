package cn.cerc.db.queue.rabbitmq;

import org.junit.Test;

public class RabbitQueueTest {

    @Test
    public void testPush() {
        try (RabbitQueue queue = new RabbitQueue(RabbitQueueTest.class.getSimpleName())) {
            for (int i = 0; i < 1000; i++) {
                queue.push(String.valueOf(i));
            }
        }
    }

    @Test
    public void testWatch() {
        try (RabbitQueue queue = new RabbitQueue(RabbitQueueTest.class.getSimpleName())) {
            for (int i = 0; i < 1000; i++) {
                queue.watch(null);
            }
        }
    }

}
