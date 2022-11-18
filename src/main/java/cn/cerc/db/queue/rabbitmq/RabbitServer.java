package cn.cerc.db.queue.rabbitmq;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.queue.AbstractQueue;
import cn.cerc.db.queue.QueueServiceEnum;

@Component
public class RabbitServer implements AutoCloseable, ApplicationListener<ApplicationContextEvent> {
    private static final Logger log = LoggerFactory.getLogger(RabbitServer.class);
    private static ConcurrentHashMap<String, RabbitQueue> items = new ConcurrentHashMap<>();
    private static RabbitServer instance;
    private Connection connection;

    public synchronized static RabbitServer get() {
        if (instance == null)
            instance = new RabbitServer();
        return instance;
    }

    // 创建连接
    private RabbitServer() {
        try {
            RabbitConfig config = new RabbitConfig();
            // 创建连接工厂
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(config.getHost());
            connectionFactory.setPort(config.getPort());
            // 设置连接哪个虚拟主机
//            connectionFactory.setVirtualHost("/test-1");
            connectionFactory.setUsername(config.getUsername());
            connectionFactory.setPassword(config.getPassword());
            this.connection = connectionFactory.newConnection();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static Channel createChannel() {
        try {
            return get().connection.createChannel();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public synchronized static RabbitQueue getQueue(String queueId) {
        RabbitQueue result = items.get(queueId);
        if (result == null) {
            result = new RabbitQueue(queueId);
            items.put(queueId, result);
        }
        return result;
    }

    @Override
    public void close() {
        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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
                Map<String, AbstractQueue> queues = context.getBeansOfType(AbstractQueue.class);
                queues.forEach((name, queue) -> {
                    if (queue.getService() == QueueServiceEnum.RabbitMQ)
                        RabbitServer.getQueue(queue.getId()).watch(queue);
                });
                log.info("成功注册的推送消息数量：" + queues.size());
            }
        } else if (event instanceof ContextClosedEvent) {
            ApplicationContext context = event.getApplicationContext();
            if (context.getParent() == null) {
                Map<String, AbstractQueue> queues = context.getBeansOfType(AbstractQueue.class);
                queues.forEach((name, queue) -> {
                    if (queue.getService() == QueueServiceEnum.RabbitMQ)
                        RabbitServer.getQueue(queue.getId()).watch(null);
                });
                log.info("关闭注册的推送消息数量：" + queues.size());
            }
        }
    }

}