package cn.cerc.db.queue;

/**
 * 阿里云消息队列
 */
public class QueueConfig {

    /**
     * 系统消息
     **/
    public static final String getMessageQueue() {
        return "message";
    }

    /**
     * 回算队列
     **/
    public static final String getSummerQueue() {
        return "summer";
    }

    /**
     * 资料同步
     **/
    public static final String getMaterialQueue() {
        return "material";
    }

    /**
     * 全文检索
     */
    public static final String getElasticsearchQueue() {
        return "elasticsearch";
    }

}
