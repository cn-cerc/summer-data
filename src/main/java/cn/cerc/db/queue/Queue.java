package cn.cerc.db.queue;

/**
 * 请改使用 QueueProxy
 * 
 * @author ZhangGong
 *
 */
@Deprecated
public class Queue extends QueueProxy implements AutoCloseable {

    public Queue(String queueId) {
        super(queueId);
    }

    @Override
    public void close() {

    }

}
