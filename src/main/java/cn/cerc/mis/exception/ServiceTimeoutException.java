package cn.cerc.mis.exception;

import cn.cerc.db.core.IHandle;

public class ServiceTimeoutException extends TimeoutException {

    public ServiceTimeoutException(IHandle handle, String dataIn, long endTime) {
        super(new String[] { handle.getCorpNo(), handle.getUserCode(), String.valueOf(endTime), dataIn });
    }

}
