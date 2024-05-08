package cn.cerc.mis.exception;

import cn.cerc.db.core.IHandle;

public class FormTimeoutException extends TimeoutException {

    public FormTimeoutException(IHandle handle, String dataIn, long endTime) {
        super(String.format("页面执行时间超过%s毫秒", Timeout));
        this.args = new String[] { handle.getCorpNo(), handle.getUserCode(), dataIn, String.valueOf(endTime) };
    }

}
