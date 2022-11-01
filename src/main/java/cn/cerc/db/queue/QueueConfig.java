package cn.cerc.db.queue;

import cn.cerc.db.core.ServerConfig;

/**
 * 阿里云消息队列
 */
public class QueueConfig {

    /**
     * TODO 临时做法
     *
     * 需要进一步改进
     */
    public final static String tag() {
        return String.format("%s-%s", ServerConfig.getAppIndustry(), ServerConfig.getAppVersion());
    }

    /**
     * 系统消息
     **/
    public static String getMessageQueue() {
        return "message";
    }

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
