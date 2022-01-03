package cn.cerc.db.core;

public interface IOption {

    String getTitle();

    String getValue(IHandle handle, String def);

    default String getValue(IHandle handle) {
        return getValue(handle, "");
    }

    default String getKey() {
        String[] items = this.getClass().getName().split("\\.");
        return items[items.length - 1];
    }

}
