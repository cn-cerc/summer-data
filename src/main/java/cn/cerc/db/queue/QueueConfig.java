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
     * 资料同步
     **/
    public static String getMaterialQueue() {
        return "material";
    }

    /**
     * 全文检索
     */
    public static String getElasticsearchQueue() {
        return "elasticsearch";
    }

}
