package cn.cerc.mq.kafka;

import java.util.HashMap;
import java.util.Map;

import cn.cerc.db.core.Utils;

public class ProducerFactory {
    public static final Map<String, IKafkaProducer> producers = new HashMap<>();

    public static IKafkaProducer getProducer(String bootstrapServer, String topic) {
        if (producers.get(topic) != null)
            return producers.get(topic);
        synchronized (producers) {
            IKafkaProducer producer = new IKafkaProducer() {

                @Override
                public String topic() {
                    return topic;
                }

                @Override
                public String bootstrapServer() {
                    if (Utils.isEmpty(bootstrapServer))
                        return super.bootstrapServer();
                    return bootstrapServer;
                }
            };
            producers.put(topic, producer);
            return producer;
        }
    }

    public static IKafkaProducer getProducer(String topic) {
        return getProducer(null, topic);
    }
}
