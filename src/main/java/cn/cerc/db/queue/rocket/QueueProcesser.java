package cn.cerc.db.queue.rocket;

public interface QueueProcesser {

    boolean processMessage(String data);

}
