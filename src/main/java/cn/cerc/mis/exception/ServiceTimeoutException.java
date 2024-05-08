package cn.cerc.mis.exception;

import cn.cerc.db.core.IHandle;

public class ServiceTimeoutException extends TimeoutException {

    public ServiceTimeoutException(IHandle handle, String dataIn, long endTime) {
        super(String.format("服务执行时间超过%s毫秒", Timeout),
                new String[] { handle.getCorpNo(), handle.getUserCode(), dataIn, String.valueOf(endTime) });
    }

}
