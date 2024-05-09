package cn.cerc.mis.exception;

public class QueueTimeoutException extends TimeoutException {

    public QueueTimeoutException(Class<?> clazz, String dataIn, long endTime) {
        super(String.format("%s 队列执行耗时 %s 毫秒", clazz.getSimpleName(), endTime));
        this.args = new String[] { clazz.getSimpleName(), dataIn, String.valueOf(endTime) };
    }

    @Override
    public String getGroup() {
        return IKnowall.消息消费慢;
    }
}
