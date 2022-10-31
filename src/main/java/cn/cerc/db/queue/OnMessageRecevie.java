package cn.cerc.db.queue;

import org.apache.rocketmq.client.apis.message.MessageView;

public interface OnMessageRecevie {

    boolean consume(MessageView message);

}
