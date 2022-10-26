package cn.cerc.db.queue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.ConsumeContext;
import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.MessageListener;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;

public class QueueConsumer {
    private Consumer consumer;
    private String topic;
    private String tag = "*";

    public QueueConsumer() {
        super();
        var properties = RocketMQ.getProperties();
        properties.put(PropertyKeyConst.GROUP_ID, "main");
        consumer = ONSFactory.createConsumer(properties);
        consumer.start();
    }

    public QueueConsumer(Consumer consumer) {
        super();
        this.consumer = consumer;
    }

    public String getTopic() {
        return topic;
    }

    public QueueConsumer setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public QueueConsumer setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public void close() {
        if (consumer != null) {
            consumer.shutdown();
            consumer = null;
        }
    }

    /**
     * 读取消息
     * 
     * @param processer
     * @return 返回读取的笔数
     */
    public int recevie(QueueProcesser processer) {
        AtomicInteger total = new AtomicInteger();
        CountDownLatch cdl = new CountDownLatch(1);
        // 若取2个tag，可这样使用：TagA||TagB
        consumer.subscribe(topic, tag, new MessageListener() {
            public Action consume(Message message, ConsumeContext context) {
                cdl.countDown();
                total.addAndGet(1);
                return processer.processMessage(new String(message.getBody())) ? Action.CommitMessage
                        : Action.ReconsumeLater;
            }
        });
        consumer.start();
        try {
            cdl.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return total.get();
    }

    public static void main(String[] args) {
        QueueProcesser processer = data -> {
            System.out.println("消息内容: " + data);
            return true;
        };

        var consumer = new QueueConsumer().setTopic("TopicTestMQ").setTag("fpl");
        var count = consumer.recevie(processer);
        consumer.close();

        System.out.println(String.format("有读到 %s 条消息", count));
    }
}
