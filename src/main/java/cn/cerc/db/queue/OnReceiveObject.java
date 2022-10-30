package cn.cerc.db.queue;

public interface OnReceiveObject<T> {
    boolean execute(T entity);
}
