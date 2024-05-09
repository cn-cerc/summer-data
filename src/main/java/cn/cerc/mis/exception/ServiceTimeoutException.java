package cn.cerc.mis.exception;

import cn.cerc.db.core.IHandle;

public class ServiceTimeoutException extends TimeoutException {

    public ServiceTimeoutException(IHandle handle, String service, String dataIn, long endTime) {
        super(String.format("%s 服务执行耗时 %s 毫秒", service, endTime));
        this.args = new String[] { handle.getCorpNo(), handle.getUserCode(), service, dataIn, String.valueOf(endTime) };
    }

    @Override
    public String getGroup() {
        return IKnowall.服务速度慢;
    }
}
