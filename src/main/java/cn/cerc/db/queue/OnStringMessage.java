package cn.cerc.db.queue;

public interface OnStringMessage {
    boolean consume(String message);
}
