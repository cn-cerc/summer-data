package cn.cerc.db.queue;

import java.time.Duration;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.zk.ZkConfig;

public abstract class AbstractQueue implements OnStringMessage, ServletContextListener, Watcher {
    private static final Logger log = LoggerFactory.getLogger(AbstractQueue.class);
    private static QueueConsumer consumer = QueueConsumer.getInstance();
    private static ZkConfig config;
    private long delayTime = 0L;

    public AbstractQueue() {
        super();
        log.debug("{} is init ", this.getClass().getSimpleName());
        // 检查消费主题、队列组是否有创建
        QueueServer.createTopic(this.getTopic(), this.getDelayTime() > 0);
    }

    public abstract String getTopic();

    public String getTag() {
        return QueueConfig.tag;
    }

    // 创建延迟队列消息
    public long getDelayTime() {
        return this.delayTime;
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        if (ServerConfig.enableTaskService()) {
            this.startService();
        } else {
            log.info("当前主机没有开启消息队列服务：{}", this.getClass().getSimpleName());
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        this.stopService();
    }

    public void startService() {
        try {
            ZkConfig host = new ZkConfig(String.format("/app/%s", ServerConfig.getAppName()));
            String child = host.path("status");
            var stat = host.client().exists(child, this);
            if (stat == null) {
                host.setValue("status", "running");
                stat = host.client().exists(child, this);
                if (stat == null) {
                    log.warn("配置有误，无法启动消息队列");
                    return;
                }
            }
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            return;
        }

        config().setTempNode(this.getClass().getSimpleName(), "running");
        log.info("注册消息推送服务：{}", this.getTopic());
        consumer.addConsumer(getTopic(), getTag(), this);
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Watcher.Event.EventType.DataWatchRemoved) {
            log.info("此主机运行状态被移除");
            this.stopService();
        }
    }

    public void stopService() {
        if (consumer == null)
            return;
        config().delete(this.getClass().getSimpleName());
        log.info("{} 关闭了消息推送服务", this.getTopic());
        consumer.close();
        consumer = null;
    }

    private ZkConfig config() {
        if (config == null)
            config = new ZkConfig(String.format("/app/%s/task", ServerConfig.getAppName()));
        return config;
    }

    protected String sendMessage(String data) {
        try {
            var producer = new QueueProducer(getTopic(), getTag());
            var messageId = producer.append(data, Duration.ofSeconds(this.delayTime));
            return messageId;
        } catch (ClientException e) {
            log.error(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
