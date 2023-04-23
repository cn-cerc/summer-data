package cn.cerc.db.queue.rabbitmq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.queue.AbstractQueue;
import cn.cerc.db.queue.QueueServiceEnum;
import cn.cerc.db.zk.ZkNode;

@Component
public class RabbitServer implements AutoCloseable, ApplicationListener<ApplicationContextEvent> {
    private static final Logger log = LoggerFactory.getLogger(RabbitServer.class);
    private static RabbitServer instance;
    private List<RabbitQueue> startItems = new ArrayList<>();
    private Connection connection;

    public synchronized static RabbitServer get() {
        if (instance == null)
            instance = new RabbitServer();
        return instance;
    }

    // 创建连接
    private RabbitServer() {
        try {
            final String prefix = String.format("/%s/%s/rabbitmq/", ServerConfig.getAppProduct(),
                    ServerConfig.getAppVersion());
            var host = ZkNode.get().getNodeValue(prefix + "host", () -> "rabbitmq.local.top");
            var port = ZkNode.get().getNodeValue(prefix + "port", () -> "5672");
            var username = ZkNode.get().getNodeValue(prefix + "username", () -> "admin");
            var password = ZkNode.get().getNodeValue(prefix + "password", () -> "admin");

            // 创建连接工厂
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(Integer.parseInt(port));
            factory.setUsername(username);
            factory.setPassword(password);

            this.connection = factory.newConnection();
            this.connection.addShutdownListener(cause -> log.info("RabbitMQ connection closed."));
        } catch (IOException | TimeoutException e) {
            log.error(e.getMessage(), e);
        }
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
//                if (!ServerConfig.enableTaskService()) {
//                    log.info("当前应用未启动消息服务与定时任务");
//                    return;
//                }
                Map<String, AbstractQueue> queues = context.getBeansOfType(AbstractQueue.class);
                queues.forEach((queueId, bean) -> {
                    if (bean.isPushMode() && bean.getService() == QueueServiceEnum.RabbitMQ) {
                        var queue = new RabbitQueue(bean.getId());
                        queue.watch(bean);
                        startItems.add(queue);
                    }
                });
                log.info("成功注册的推送消息数量：" + startItems.size());
            }
        } else if (event instanceof ContextClosedEvent) {
            for (var queue : startItems)
                queue.watch(null);
            log.info("关闭注册的推送消息数量：" + startItems.size());
            startItems.clear();
        }
    }

    public Connection getConnection() {
        return this.connection;
    }

}