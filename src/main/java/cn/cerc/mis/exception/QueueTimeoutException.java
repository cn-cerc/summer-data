package cn.cerc.mis.exception;

public class QueueTimeoutException extends TimeoutException {

    public QueueTimeoutException(String dataIn, long endTime) {
        super(String.format("队列执行时间超过%s毫秒", Timeout));
        this.args = new String[] { dataIn, String.valueOf(endTime) };
    }

}
