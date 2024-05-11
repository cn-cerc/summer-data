package cn.cerc.mis.exception;

import cn.cerc.db.core.IHandle;

public class ServiceTimeoutException extends TimeoutException {

    private static final long serialVersionUID = 4524226678058812873L;

    public ServiceTimeoutException(IHandle handle, String service, String dataIn, long endTime) {
        super(String.format("%s 服务执行耗时 %s 毫秒", service, endTime));
        this.args = new String[] { handle.getCorpNo(), handle.getUserCode(), service, dataIn, String.valueOf(endTime) };
    }

}
