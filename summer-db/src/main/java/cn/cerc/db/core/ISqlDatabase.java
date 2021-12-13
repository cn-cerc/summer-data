package cn.cerc.db.core;

public interface ISqlDatabase {

    boolean createTable(boolean overwrite);

    String oid();

    String table();
}
