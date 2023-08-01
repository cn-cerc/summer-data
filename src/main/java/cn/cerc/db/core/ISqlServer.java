package cn.cerc.db.core;

public interface ISqlServer extends IConnection {

    boolean execute(String sql);

    String getHost();

}
