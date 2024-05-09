package cn.cerc.mis.exception;

import cn.cerc.db.core.IHandle;

public class ServiceTimeoutException extends TimeoutException {

    public ServiceTimeoutException(IHandle handle, String dataIn, long endTime) {
        super(String.format("服务执行耗时 %s 毫秒", endTime));
        this.args = new String[] { handle.getCorpNo(), handle.getUserCode(), dataIn, String.valueOf(endTime) };
    }

    @Override
    public String getGroup() {
        return IKnowall.服务速度慢;
    }
}
