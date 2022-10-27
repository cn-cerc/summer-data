package cn.cerc.db.queue;

import cn.cerc.db.core.ClassConfig;
import cn.cerc.db.core.ServerConfig;
import cn.cerc.db.core.Utils;

/**
 * 阿里云消息队列
 */
public class QueueConfig {

    private static final ClassConfig config = new ClassConfig(QueueConfig.class, null);

    /**
     * TODO 临时做法
     * 
     * 需要进一步改进
     */
    public final static String tag = String.format("%s-%s",
            ServerConfig.getInstance().getProperty("application.original"),
            ServerConfig.getInstance().getProperty("version"));

    /**
     * 系统消息
     **/
    public static final String getMessageQueue() {
        String queue = config.getProperty("application.queue.message");
        if (Utils.isEmpty(queue))
            throw new RuntimeException("the queue key application.queue.message is empty");
        return queue;
    }

    /**
     * 资料同步
     **/
    public static final String getMaterialQueue() {
        String queue = config.getProperty("application.queue.material");
        if (Utils.isEmpty(queue))
            throw new RuntimeException("the queue key application.queue.material is empty");
        return queue;
    }

    /**
     * 全文检索
     */
    public static final String getElasticsearchQueue() {
        String queue = config.getProperty("application.queue.elasticsearch");
        if (Utils.isEmpty(queue))
            throw new RuntimeException("the queue key application.queue.elasticsearch is empty");
        return queue;
    }

}
