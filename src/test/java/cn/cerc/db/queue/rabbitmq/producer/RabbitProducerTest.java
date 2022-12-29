package cn.cerc.db.queue.rabbitmq.producer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RabbitProducerTest {

    private static final int MAX_THREAD_SIZE = Runtime.getRuntime().availableProcessors();
    private static final ExecutorService pool = Executors.newFixedThreadPool(MAX_THREAD_SIZE);

    public static void main(String[] args) {
//        new ProducerThread().run();
        for (int i = 0; i < MAX_THREAD_SIZE; i++) {
            pool.submit(new ProducerThread());
        }
    }

}
