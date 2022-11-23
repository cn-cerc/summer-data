package cn.cerc.db.queue.rabbitmq.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RabbitConsumerTest {

    private static final int MAX_THREAD_SIZE = Runtime.getRuntime().availableProcessors();
    private static final ExecutorService pool = Executors.newFixedThreadPool(MAX_THREAD_SIZE);

    public static void main(String[] args) {
        for (int i = 0; i < 2; i++) {
            pool.submit(new RabbitConsumeThread());
        }
    }

}