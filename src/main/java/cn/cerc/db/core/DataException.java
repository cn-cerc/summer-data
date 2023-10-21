package cn.cerc.db.core;

/**
 * 数据集异常
 */
public abstract class DataException extends Exception {

    private static final long serialVersionUID = -8562304524460232684L;

    public DataException() {
    }

    public DataException(Exception e) {
        super(e.getMessage());
        this.addSuppressed(e);
    }

    public DataException(String message) {
        super(message);
    }

}
