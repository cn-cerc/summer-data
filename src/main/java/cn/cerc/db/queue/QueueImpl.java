package cn.cerc.db.queue;

import com.aliyun.mns.client.CloudQueue;

public interface QueueImpl {

    String getQueueId();

    CloudQueue getQueue();

}
