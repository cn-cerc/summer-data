package cn.cerc.mis.exception;

import cn.cerc.db.core.IHandle;

public class FormTimeoutException extends TimeoutException {

    public FormTimeoutException(IHandle handle, String dataIn, long endTime) {
        super(String.format("页面执行耗时 %s 毫秒", endTime));
        this.args = new String[] { handle.getCorpNo(), handle.getUserCode(), dataIn, String.valueOf(endTime) };
    }

    @Override
    public String getGroup() {
        return IKnowall.页面加载慢;
    }
}
