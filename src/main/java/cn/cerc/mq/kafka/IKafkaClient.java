package cn.cerc.mq.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public abstract class IKafkaClient implements AutoCloseable {
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
        while (true) {
            ConsumerRecords<String, String> records = client.poll(Duration.ZERO);
            for (ConsumerRecord<String, String> record : records) {
                process(record.value());
                client.commitSync();
            }
        }
    }

    protected abstract void process(String record);

    @Override
    public void close() throws Exception {
        if (client != null)
            client.close();
    }
}
