package cn.cerc.mis.exception;

import cn.cerc.db.core.ClassConfig;

public abstract class TimeoutException extends Exception implements IKnowall {
    private static final long serialVersionUID = 3430308471296626833L;

    public static final int Timeout = new ClassConfig().getInt("performance.monitor.timeout", 3000);

    protected String[] args;

    public TimeoutException(String message) {
        super(message);
    }

    @Override
    public String[] getData() {
        return args;
    }

}
