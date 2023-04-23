package cn.cerc.db.sms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.dysmsapi20170525.Client;
import com.aliyun.dysmsapi20170525.models.SendSmsRequest;
import com.aliyun.dysmsapi20170525.models.SendSmsResponseBody;
import com.aliyun.teaopenapi.models.Config;
import com.google.gson.Gson;

import cn.cerc.db.SummerDB;
import cn.cerc.db.core.ClassResource;

public class Aliyundysms {
    private static final ClassResource res = new ClassResource(Aliyundysms.class, SummerDB.ID);
    private static final Logger log = LoggerFactory.getLogger(Aliyundysms.class);

    // 环境配置
    private final String accessKeyId;
    private final String accessSecret;
    // 签名模版
    private final String signName;
    // 接收手机
    private String phoneNumbers;
    // 模版编号
    private String templateCode;
    // 模版内容
    private String templateParam;
    // 扩展字段
    private String outId;
    // 执行结果
    private String message;

    public Aliyundysms(String signName, String accessKeyId, String accessSecret) {
        this.signName = signName;
        this.accessKeyId = accessKeyId;
        this.accessSecret = accessSecret;
    }

    public boolean send(String outId, String templateParam) {
        // 设置超时时间
        System.setProperty("sun.net.client.defaultConnectTimeout", "10000");
        System.setProperty("sun.net.client.defaultReadTimeout", "10000");

        if (accessKeyId == null) {
            this.message = res.getString(1, "无法读取短信发送配置：accessKeyId！");
            return false;
        }

        if (accessSecret == null) {
            this.message = res.getString(2, "无法读取短信发送配置：appSercret！");
            return false;
        }

        if (signName == null) {
            this.message = res.getString(3, "签名模版不允许为空");
            return false;
        }

        if (phoneNumbers == null) {
            this.message = res.getString(4, "电话号码不允许为空");
            return false;
        }

        if (templateCode == null) {
            this.message = res.getString(5, "短信模版不允许为空");
            return false;
        }

        try {
            Client client = Aliyundysms.createClient(accessKeyId, accessSecret);
            SendSmsRequest request = new SendSmsRequest();
            request.setSignName(signName);
            request.setPhoneNumbers(phoneNumbers);
            request.setTemplateCode(templateCode);
            request.setTemplateParam(templateParam);
            request.setOutId(outId);

            SendSmsResponseBody response = client.sendSms(request).getBody();
            log.info("----------------阿里云短信接口返回的数据----------------");
            log.info("发送请求 {} ，服务响应 {}", new Gson().toJson(this), response.getMessage());
            log.info("Code={}", response.getCode());
            log.info("Message={}", response.getMessage());
            log.info("RequestId={}", response.getRequestId());
            log.info("BizId={}", response.getBizId());
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return true;
    }

    public static Client createClient(String accessKeyId, String accessKeySecret) throws Exception {
        Config config = new Config().setAccessKeyId(accessKeyId).setAccessKeySecret(accessKeySecret);
        config.endpoint = "dysmsapi.aliyuncs.com";
        return new Client(config);
    }

    public String getMessage() {
        return message;
    }

    public String getPhoneNumbers() {
        return phoneNumbers;
    }

    public void setPhoneNumbers(String phoneNumbers) {
        this.phoneNumbers = phoneNumbers;
    }

    public String getTemplateCode() {
        return templateCode;
    }

    public void setTemplateCode(String templateCode) {
        this.templateCode = templateCode;
    }

    public String getTemplateParam() {
        return templateParam;
    }

    public void setTemplateParam(String templateParam) {
        this.templateParam = templateParam;
    }

    public String getOutId() {
        return outId;
    }

    public void setOutId(String outId) {
        this.outId = outId;
    }

}
