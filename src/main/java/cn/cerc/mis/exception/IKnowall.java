package cn.cerc.mis.exception;

public interface IKnowall {

    /**
     * @return 附加数据
     */
    String[] getData();

    /**
     * @return 内置分组
     */
    default String getGroup() {
        return this.getClass().getSimpleName();
    }

}
