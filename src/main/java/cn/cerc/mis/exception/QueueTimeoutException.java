package cn.cerc.mis.exception;

public class QueueTimeoutException extends TimeoutException {

    public QueueTimeoutException(String dataIn, long endTime) {
        super(new String[] { dataIn, String.valueOf(endTime) });
    }

}
