package cn.cerc.db.rabbitmq;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.queue.rabbitmq.RabbitQueue;
import cn.cerc.db.queue.rabbitmq.RabbitServer;

public class RabbitConsumerTest {

    private static final Logger log = LoggerFactory.getLogger(RabbitConsumerTest.class);

    private static final int MAX_THREAD_SIZE = Runtime.getRuntime().availableProcessors() * 3;
    private static ExecutorService pool = Executors.newFixedThreadPool(MAX_THREAD_SIZE);

    @Test
    public void test() {
        RabbitQueue queue = RabbitServer.getQueue("work");
        queue.watch(message -> {
            pool.submit(() -> {
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                }
                log.info(message);
            });
            return true;
        });
        try {
            Thread.sleep(1000 * 60 * 60);
        } catch (Exception e) {
        }
    }

}
