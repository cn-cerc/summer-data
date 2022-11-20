package cn.cerc.db.queue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import cn.cerc.db.core.ServerConfig;

@Component
public class QueueConsumer implements AutoCloseable, ApplicationListener<ApplicationContextEvent>, OnMessageRecevie {
    private static final Logger log = LoggerFactory.getLogger(QueueConsumer.class);
    private static final Map<String, OnStringMessage> items1 = new HashMap<>();
    private static final Map<String, FilterExpression> items2 = new HashMap<>();
    private List<AbstractQueue> startItems = new ArrayList<>();
    private PushConsumer pushConsumer;
    private SimpleConsumer pullConsumer;

    public QueueConsumer() {
        super();
    }

    @Override
    public void close() {
        if (pushConsumer != null) {
            try {
                pushConsumer.close();
                pushConsumer = null;
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
        if (pullConsumer != null) {
            try {
                pullConsumer.close();
                pullConsumer = null;
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void addConsumer(String topic, String tag, OnStringMessage event) {
        String key = topic + "-" + tag;
        log.debug("register consumer: {}", key);
        items1.put(key, event);
        items2.put(topic, new FilterExpression(tag, FilterExpressionType.TAG));
    }

    @Override
    public boolean consume(MessageView message) {
        String key = message.getTopic() + "-" + message.getTag().orElse(null);
        log.info("收到一条消息：{}", key);
        String data = StandardCharsets.UTF_8.decode(message.getBody()).toString();
        var event = items1.get(key);
        if (event == null) {
            log.error("未注册消息对象{}, data:", key, data);
            return true;
        }
        return event.consume(data);
    }

    @Override
    public void onApplicationEvent(ApplicationContextEvent event) {
        if (event instanceof ContextRefreshedEvent) {
            ApplicationContext context = event.getApplicationContext();
            if (context.getParent() == null) {
                if (!ServerConfig.enableTaskService()) {
                    log.info("当前应用未启动消息服务与定时任务");
                    return;
                }
                log.info("成功注册的 push 消息数量 {}", startItems.size());
            }
        } else if (event instanceof ContextClosedEvent) {
            for (var queue : startItems)
                queue.stopService();
            log.info("关闭注册的 push 消息数量 {}", startItems.size());
            startItems.clear();
        }
    }

    public String getGroupId() {
        return String.format("%s-%s-%s", ServerConfig.getAppProduct(), ServerConfig.getAppIndustry(),
                ServerConfig.getAppVersion());
    }

}
