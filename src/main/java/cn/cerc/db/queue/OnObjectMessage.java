package cn.cerc.db.queue;

public interface OnObjectMessage<T> {
    boolean execute(T entity);
}
