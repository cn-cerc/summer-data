package cn.cerc.mq.kafka;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.ServerConfig;

/**
 * kafka-producer抽象类
 * 
 */
public abstract class IKafkaProducer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(IKafkaProducer.class);

    /**
     * kafka producer
     */
    private KafkaProducer<String, String> producer;

    /**
     * 订阅的主题
     */
    public abstract String topic();

    /**
     * kafka地址,读取配置文件 kafka.bootstrapServer,形如 192.168.1.1:9092
     */
    public String bootstrapServer() {
        return ServerConfig.getInstance().getProperty("kafka.bootstrapServer");
    }

    public IKafkaProducer() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer());
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        producer = new KafkaProducer<>(prop);
    }

    /**
     * 发送带key的消息并传入回调函数，回调也是异步
     * 
     * @param partition
     * @param key
     * @param value
     * @param callback
     * @return
     */
    public Future<RecordMetadata> send(Integer partition, String key, String value, Callback callback) {
        return producer.send(new ProducerRecord<String, String>(topic(), partition, key, value), callback);
    }

    /**
     * 发送不带key的消息并传入回调函数，回调也是异步
     * 
     * @param partition
     * @param value
     * @param callback
     * @return
     */
    public Future<RecordMetadata> send(Integer partition, String value, Callback callback) {
        return send(partition, null, value, callback);
    }

    /**
     * 发送带key的消息
     * 
     * @param partition
     * @param key
     * @param value
     * @return
     */
    public Future<RecordMetadata> send(Integer partition, String key, String value) {
        return send(partition, key, value, null);
    }

    /**
     * 发送带key的消息并传入回调函数，回调也是异步
     * 
     * @param key
     * @param value
     * @param callback
     * @return
     */
    public Future<RecordMetadata> send(String key, String value, Callback callback) {
        return send(null, key, value, callback);
    }

    /**
     * 发送带key的消息
     * 
     * @param key
     * @param value
     * @return
     */
    public Future<RecordMetadata> send(String key, String value) {
        return send(null, key, value, null);
    }

    /**
     * 发送不带key的消息
     * 
     * @param partition
     * @param value
     * @return
     */
    public Future<RecordMetadata> send(Integer partition, String value) {
        return send(null, null, value, null);
    }

    /**
     * 发送不带key的消息
     * 
     * @param callback
     * @param value
     * @return
     */
    public Future<RecordMetadata> send(String value, Callback callback) {
        return send(null, null, value, callback);
    }

    /**
     * 发送不带key的消息
     * 
     * @param value
     * @return
     */
    public Future<RecordMetadata> send(String value) {
        return send(null, null, value, null);
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            producer.close();
        }
    }
}
