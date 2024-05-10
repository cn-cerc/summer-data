package cn.cerc.mis.exception;

import cn.cerc.db.core.IHandle;

public class FormTimeoutException extends TimeoutException {
    private static final long serialVersionUID = 1258357466487554879L;

    public FormTimeoutException(IHandle handle, String formId, String dataIn, long endTime) {
        super(String.format("%s 页面执行耗时 %s 毫秒", formId, endTime));
        this.args = new String[] { handle.getCorpNo(), handle.getUserCode(), dataIn, String.valueOf(endTime) };
    }

    @Override
    public String getGroup() {
        return IKnowall.Form执行慢;
    }
}
