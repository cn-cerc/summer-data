package cn.cerc.db.core;

public interface IOption {
    public static final String ON = "on";
    public static final String OFF = "off";

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
