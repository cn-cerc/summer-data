package cn.cerc.db.jiguang;

import cn.jiguang.common.resp.APIConnectionException;
import cn.jiguang.common.resp.APIRequestException;
import cn.jpush.api.push.PushResult;
import cn.jpush.api.push.model.Message;
import cn.jpush.api.push.model.Options;
import cn.jpush.api.push.model.Platform;
import cn.jpush.api.push.model.PushPayload;
import cn.jpush.api.push.model.PushPayload.Builder;
import cn.jpush.api.push.model.audience.Audience;
import cn.jpush.api.push.model.notification.AndroidNotification;
import cn.jpush.api.push.model.notification.IosAlert;
import cn.jpush.api.push.model.notification.IosNotification;
import cn.jpush.api.push.model.notification.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class JPushBuilder {

    private static final Logger log = LoggerFactory.getLogger(JPushBuilder.class);

    /**
     * 消息标题，仅安卓机型有效，IOS设备忽略，默认为应用标题
     */
    private String title;
    /**
     * 消息内容
     */
    private String message;
    /**
     * 消息声音--app需要有音源
     */
    private String sound = "default";
    /**
     * 附加参数
     */
    private final Map<String, String> extras = new HashMap<>();

    /**
     * 发送给指定设备
     *
     * @param alias 设备id，对应极光推送的设备别名
     */
    public void send(String... alias) {
        if (alias == null || alias.length == 0)
            return;

        // 发送给指定的设备
        Builder builder = PushPayload.newBuilder();
        builder.setAudience(Audience.alias(alias));
        builder.setPlatform(Platform.android_ios());

        IosAlert iosAlert = IosAlert.newBuilder()
                .setTitleAndBody(this.title, "", this.message)
                .build();

        builder.setNotification(Notification.newBuilder()
                        .addPlatformNotification(
                                AndroidNotification.newBuilder().setAlert(message)
                                        .setTitle(this.title)
                                        .addExtras(this.extras)
                                        .build())
                        .addPlatformNotification(
                                IosNotification.newBuilder().setAlert(iosAlert)
                                        .incrBadge(1)
                                        .addExtras(this.extras)
                                        .setSound(this.sound)
                                        .build())
                        .build())
                .build();
        // 设置生产环境 iOS 平台专用
        builder.setOptions(Options.newBuilder().setApnsProduction(true).build()).build();
        PushPayload payload = builder.build();
        try {
            PushResult result = JPushConfig.getClient().sendPush(payload);
            log.info("Got result - " + result);
        } catch (APIConnectionException e) {
            log.error("Connection error, should retry later", e);
        } catch (APIRequestException e) {
            log.error("Should review the error, and fix the request", e);
            log.info("HTTP Status: " + e.getStatus());
            log.info("Error Code: " + e.getErrorCode());
            log.info("Error Message: " + e.getErrorMessage());
            log.info("PushPayload Message: " + payload);
        }
    }

    public String getTitle() {
        return title;
    }

    public JPushBuilder setTitle(String title) {
        this.title = title;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public JPushBuilder setMessage(String message) {
        this.message = message;
        return this;
    }

    public String getSound() {
        return sound;
    }

    public void setSound(String sound) {
        this.sound = sound;
    }

    /**
     * 增加附加参数到 extras
     */
    public JPushBuilder addExtra(String key, String value) {
        extras.put(key, value);
        return this;
    }

}
