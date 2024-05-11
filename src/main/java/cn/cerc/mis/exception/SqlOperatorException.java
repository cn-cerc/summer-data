package cn.cerc.mis.exception;

import cn.cerc.db.core.IHandle;

public class SqlOperatorException extends RuntimeException implements IKnowall {

    private static final long serialVersionUID = 1L;

    private String[] data;

    public SqlOperatorException(IHandle handle, String message, String dataIn, String sqlText) {
        super(message);
        this.data = new String[] { handle.getCorpNo(), handle.getUserCode(), dataIn, sqlText };
    }

    @Override
    public String[] getData() {
        return data;
    }

    @Override
    public String getGroup() {
        return IKnowall.记录被其他用户修改;
    }

}
