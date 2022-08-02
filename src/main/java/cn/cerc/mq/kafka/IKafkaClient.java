package cn.cerc.mq.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IKafkaClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(IKafkaClient.class);

    private KafkaConsumer<String, String> client;

    public abstract String topic();

    public abstract String consumerGroup();

    public abstract String bootstrapServer();

    public IKafkaClient() {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer());
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup());

        client = new KafkaConsumer<String, String>(prop);
        client.subscribe(Collections.singleton(topic()));
        startListen();
    }

    public void startListen() {
        log.info("bootstrapServer {} topic {} consumerGroup {} start listening", bootstrapServer(), topic(),
                consumerGroup());
        new Thread(() -> {
            while (true) {
                try {
                    ConsumerRecords<String, String> records = client.poll(Duration.ZERO);
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            process(record.value());
                        } catch (Exception e) {
                            log.error("消息消费失败 {} {}", record.topic(), record.value());
                            // TODO: 后期做消费补偿机制
                        }
                        client.commitSync();
                    }
                } catch (WakeupException e) {
                    break;
                }
            }
            log.info("bootstrapServer {} topic {} consumerGroup {} end listening", bootstrapServer(), topic(),
                    consumerGroup());
            client.close();
        }).start();
    }

    protected abstract void process(String record);

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.wakeup();
        }
    }
}
