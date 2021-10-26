package cn.cerc.db.core;

public interface SqlServer extends IConnection {

    boolean execute(String sql);
    
    SqlOperator getDefaultOperator(IHandle handle);

}
