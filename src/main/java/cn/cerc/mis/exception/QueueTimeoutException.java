package cn.cerc.mis.exception;

public class QueueTimeoutException extends TimeoutException {

    public QueueTimeoutException(String dataIn, long endTime) {
        super(String.format("队列执行耗时 %s 毫秒", endTime));
        this.args = new String[] { dataIn, String.valueOf(endTime) };
    }

    @Override
    public String getGroup() {
        return IKnowall.消息消费慢;
    }
}
