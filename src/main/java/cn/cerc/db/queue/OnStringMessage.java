package cn.cerc.db.queue;

public interface OnStringMessage {

    /**
     * 
     * @param message       消息内容
     * @param repushOnError 是否在失败时进行转发(Sqlmq中继续执行)
     * @return true or false
     */
    boolean consume(String message, boolean repushOnError);
}
