package cn.cerc.db.queue;

public interface QueueProcesser {

    boolean processMessage(String data);

}
