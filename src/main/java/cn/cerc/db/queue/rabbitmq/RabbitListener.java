package cn.cerc.db.queue.rabbitmq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.queue.AbstractQueue;
import cn.cerc.db.queue.QueueServiceEnum;

@Component
public class RabbitListener implements ApplicationListener<ApplicationContextEvent> {
    private static final Logger log = LoggerFactory.getLogger(RabbitListener.class);
    private List<RabbitQueue> startItems = new ArrayList<>();

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
                try {
                    queues.forEach((queueId, bean) -> {
                        if (bean.isPushMode() && bean.getService() == QueueServiceEnum.RabbitMQ) {
                            var queue = new RabbitQueue(bean.getId());
                            queue.watch(bean);
                            startItems.add(queue);
                        }
                    });
                    log.info("成功注册的推送消息数量 {}", startItems.size());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        } else if (event instanceof ContextClosedEvent) {
            for (var queue : startItems)
                queue.watch(null);
            log.info("关闭注册的推送消息数量 {}", startItems.size());
            startItems.clear();
        }
    }

}