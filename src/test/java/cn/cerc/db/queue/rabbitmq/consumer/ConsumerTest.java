package cn.cerc.db.queue.rabbitmq.consumer;

import cn.cerc.db.queue.rabbitmq.RabbitQueue;
import cn.cerc.db.queue.rabbitmq.config.RabbitTestConfig;

public class ConsumerTest {

//    @Test
    public void test_consume_1() {
        try (RabbitQueue queue = new RabbitQueue(RabbitTestConfig.QUEUE_NAME)) {
            queue.setMaximum(15);
            queue.pop((msg, repushOnError) -> {
                System.out.println(msg);
                return true;
            });
        }
    }

//    @Test
    public void test_consume_2() throws InterruptedException {
        try (RabbitQueue queue = new RabbitQueue(RabbitTestConfig.QUEUE_NAME)) {
            queue.setMaximum(10);
            queue.watch((msg, repushOnError) -> {
                System.out.println(msg);
                return true;
            });
            Thread.sleep(1000);
        }
    }

}
