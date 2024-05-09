package cn.cerc.mis.exception;

public interface IKnowall {
    String 权限不足 = "110001";
    String 数据校验错误 = "110002";
    String 记录被其他用户 = "110003";
    String 远程服务超时 = "110004";
    String 找不到页面异常 = "110005";
    String 数据库异常 = "110006";
    String 服务速度慢 = "120001";
    String 消息消费慢 = "120002";
    String 页面加载慢 = "120003";
    String 前端调用慢 = "120004";

    /**
     * @return 附加数据
     */
    String[] getData();

    /**
     * @return 内置分组
     */
    String getGroup();

}
