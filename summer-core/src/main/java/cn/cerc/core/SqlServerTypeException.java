package cn.cerc.core;

public class SqlServerTypeException extends RuntimeException  {
    private static final long serialVersionUID = 4300770778224785368L;
    
    public SqlServerTypeException() {
        super("not support this sqlServer type");
    }
    
    public SqlServerTypeException(String message) {
        super(message);
    }
}
