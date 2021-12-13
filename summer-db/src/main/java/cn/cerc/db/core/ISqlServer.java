package cn.cerc.db.core;

import cn.cerc.core.IConnection;

public interface ISqlServer extends IConnection {

    boolean execute(String sql);
    
    String getHost();
}
