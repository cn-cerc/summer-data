package cn.cerc.db.core;

public interface DataSource {

    /**
     * 
     * @return 返回数据是否只读，默认值为 true
     */
    boolean readonly();
}
