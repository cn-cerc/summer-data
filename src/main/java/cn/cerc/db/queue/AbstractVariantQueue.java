package cn.cerc.db.queue;

public abstract class AbstractVariantQueue extends AbstractQueue {

    public void append(String data) {
        super.sendMessage(data);
    }

}
