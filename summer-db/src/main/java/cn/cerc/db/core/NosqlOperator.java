package cn.cerc.db.core;

public interface NosqlOperator {

    boolean insert(DataRow record);

    boolean update(DataRow record);

    boolean delete(DataRow record);

}
