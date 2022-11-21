package cn.cerc.db.rabbitmq;

import org.junit.Test;

import cn.cerc.db.core.Datetime;
import cn.cerc.db.queue.rabbitmq.RabbitQueue;
import cn.cerc.db.queue.rabbitmq.RabbitServer;

public class RabbitProducesTest {

    @Test
    public void test() {
        RabbitQueue queue = RabbitServer.getQueue("work");
        for (int i = 0; i < 100000; i++) {
            queue.push(String.format("%s: 消息产生于%s", i, new Datetime()));
        }
    }

}
