package cn.cerc.db.log;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import cn.cerc.db.core.ClassConfig;
import cn.cerc.db.core.Curl;
import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.Utils;

public class KnowallLog {
    /**
     * 获取当前JVM运行环境可调用的处理器线程数
     */
    private static final int processors = Runtime.getRuntime().availableProcessors();

    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(processors, processors, 60,
            TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024), new ThreadPoolExecutor.CallerRunsPolicy());
    private static final ClassConfig config = new ClassConfig();

    private long createTime;
    private String origin;
    private String level;
    private String message;
    private String type;
    private ArrayList<String> data;
    private String machine;

    public KnowallLog(Class<?> clazz, int line) {
        this(String.format("%s:%s", clazz.getName(), line));
    }

    public KnowallLog(String origin) {
        this.origin = origin;
        this.createTime = System.currentTimeMillis();
        this.level = "info";
        try {
            InetAddress inet = InetAddress.getLocalHost();
            this.machine = inet.getHostName();
        } catch (UnknownHostException e) {
            System.err.println(e.getMessage());
        }
    }

    public static KnowallData of(String... data) {
        KnowallData knowallData = new KnowallData();
        if (data == null)
            return knowallData;
        for (String val : data) {
            knowallData.addData(val);
        }
        return knowallData;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getMachine() {
        return machine;
    }

    public void setMachine(String machine) {
        this.machine = machine;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public KnowallLog addData(String data) {
        if (this.data == null)
            this.data = new ArrayList<>();
        if (this.data.size() < 10)
            this.data.add(data);
        return this;
    }

    public String getData(int index) {
        return data.get(index);
    }

    public int getDataCount() {
        return data == null ? 0 : data.size();
    }

    public void post() {
        post(null);
    }

    public void post(Consumer<String> callBack) {
        String site = config.getString("cn.knowall.site", "");
        if (Utils.isEmpty(site))
            return;

        String profile = "cn.knowall.token";
        String token = config.getString(profile, "");
        if (Utils.isEmpty(token)) {
            System.err.println(String.format("项目日志配置 %s 为空", profile));
            return;
        }

        executor.submit(() -> {
            try {
                Curl curl = new Curl();
                curl.put("origin", this.origin);
                curl.put("message", this.message);
                curl.put("level", this.level);
                if (this.type != null)
                    curl.put("type", this.type);
                if (this.machine != null)
                    curl.put("machine", this.machine);
                curl.put("createTime", this.createTime);
                if (this.data != null) {
                    for (int i = 0; i < this.data.size(); i++) {
                        String val = this.data.get(i);
                        curl.put("data" + i, val);
                    }
                }
                String response = curl.doPost(String.format("%s/public/log1?token=%s", site, token));
                if (Utils.isEmpty(response))
                    System.err.println(String.format("token: %s, json: %s", token, message));
                else {
                    DataRow row = new DataRow().setJson(response);
                    if (!row.getBoolean("result"))
                        System.err.println(row.getString("message"));
                    if (callBack != null)
                        callBack.accept(response);
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        });
    }
}
