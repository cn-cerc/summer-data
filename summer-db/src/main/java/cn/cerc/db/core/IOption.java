package cn.cerc.db.core;

public interface IOption {

    String getTitle();

    String getValue(IHandle handle);
    
    default String getDefault() {
        return "";
    }

    default String getKey() {
        String[] items = this.getClass().getName().split("\\.");
        return items[items.length - 1];
    }

}
