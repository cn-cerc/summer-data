package cn.cerc.mis.exception;

import cn.cerc.db.core.IHandle;

public class FormTimeoutException extends TimeoutException {

    public FormTimeoutException(IHandle handle, String dataIn, long endTime) {
        super(new String[] { handle.getCorpNo(), handle.getUserCode(), dataIn, String.valueOf(endTime) });
    }

}
