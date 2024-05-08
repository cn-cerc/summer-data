package cn.cerc.mis.exception;

import cn.cerc.db.core.ClassConfig;

public abstract class TimeoutException extends Exception implements IJayunArgsException {

    public static final int Timeout = new ClassConfig().getInt("performance.monitor.timeout", 1000);

    protected String[] args;

    public TimeoutException(String message, String[] args) {
        super(message);
        this.args = args;
    }

    @Override
    public String[] getArgs() {
        return args;
    }

}
