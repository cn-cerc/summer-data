package cn.cerc.db.queue;

/**
 * 阿里云消息队列
 */
public interface IQueueConfig {

    /**
     * 系统消息
     **/
    default String getMessageQueue() {
        return "message";
    };

    /**
     * 回算队列
     **/
    default String getSummerQueue() {
        return "summer";
    }

    /**
     * 资料同步
     **/
    default String getMaterialQueue() {
        return "material";
    }

    /**
     * 全文检索
     */
    default String getElasticsearchQueue() {
        return "elasticsearch";
    }

}
