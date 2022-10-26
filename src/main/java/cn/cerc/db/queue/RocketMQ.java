package cn.cerc.db.queue;

import java.util.Properties;

import com.aliyun.openservices.ons.api.PropertyKeyConst;

public class RocketMQ {

    static Properties getProperties() {
//        IConfig config = ServerConfig.getInstance();
        String endpoint = "rmq-cn-i7m2xizv402.cn-shenzhen.rmq.aliyuncs.com:8080";
        String accessId = "IVIEZ8xKfS7r82Uf";
        String password = "iJpc39B7qoEJZYt9";

        Properties properties = new Properties();
        /**
         * 如果是使用公网接入点访问，则必须设置AccessKey和SecretKey，里面填写实例的用户名和密码。实例用户名和密码在控制台实例详情页面获取。
         * 注意！！！这里填写的不是阿里云账号的AccessKey ID和AccessKey Secret，请务必区分开。
         * 如果是在阿里云ECS内网访问，则无需配置，服务端会根据内网VPC信息智能获取。
         */
        // 设置为消息队列RocketMQ版控制台实例详情页的实例用户名。
        properties.put(PropertyKeyConst.AccessKey, accessId);
        // 设置为消息队列RocketMQ版控制台实例详情页的实例密码。
        properties.put(PropertyKeyConst.SecretKey, password);
        // 注意！！！使用ONS SDK访问RocketMQ 5.0实例时，InstanceID属性不需要设置，否则会导致失败。

        // 设置发送超时时间，单位：毫秒。
        properties.setProperty(PropertyKeyConst.SendMsgTimeoutMillis, "3000");
        // 设置为您从消息队列RocketMQ版控制台获取的接入点，类似“rmq-cn-XXXX.rmq.aliyuncs.com:8080”。
        // 注意！！！直接填写控制台提供的域名和端口即可，请勿添加http://或https://前缀标识，也不要用IP解析地址。
        properties.put(PropertyKeyConst.NAMESRV_ADDR, endpoint);
        return properties;
    }

}
