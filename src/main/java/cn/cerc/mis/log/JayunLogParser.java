package cn.cerc.mis.log;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.DefaultThrowableRenderer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cn.cerc.db.core.DataException;
import cn.cerc.db.core.Utils;
import cn.cerc.mis.core.LastModified;
import cn.cerc.mis.log.JayunLogData.Builder;

/**
 * 异常解析器用于读取堆栈的异常对象信息
 */
public class JayunLogParser {
    private static final ExecutorService executor = Executors.newSingleThreadExecutor();
    private static volatile JayunLogParser instance;
    private final String loggerName;

    /**
     * 通缉名单
     */
    private static final Set<String> wanted = new HashSet<>();
    static {
        wanted.add("site.diteng");
        wanted.add("site.obm");
        wanted.add("site.oem");
        wanted.add("site.odm");
        wanted.add("site.fpl");
    }

    private JayunLogParser() {
        String loggerName = "";
        PropertyConfigurator.configure(JayunLogParser.class.getClassLoader().getResource("log4j.properties"));
        Logger logger = Logger.getRootLogger();
        Enumeration<?> allAppends = logger.getAllAppenders();
        Iterator<?> asIterator = allAppends.asIterator();
        while (asIterator.hasNext()) {
            if (asIterator.next() instanceof JayunLogAppender jayun) {
                loggerName = jayun.getName();
                break;
            }
        }
        this.loggerName = loggerName;
    }

    public String getLoggerName() {
        return loggerName;
    }

    /**
     * 用于反向获取 log4j.properties 配置的 JayunLog 别名
     */
    public static String loggerName() {
        if (instance == null) {
            synchronized (JayunLogParser.class) {
                if (instance == null)
                    instance = new JayunLogParser();
            }
        }
        return instance.getLoggerName();
    }

    /**
     * 警告类日志
     */
    public static void warn(Class<?> clazz, Throwable throwable, String message) {
        JayunLogParser.analyze(clazz, throwable, JayunLogData.warn, message);
    }

    /**
     * 警告类日志
     */
    public static void warn(Class<?> clazz, Throwable throwable) {
        JayunLogParser.analyze(clazz, throwable, JayunLogData.warn, null);
    }

    /**
     * 错误类日志
     */
    public static void error(Class<?> clazz, Throwable throwable, String message) {
        JayunLogParser.analyze(clazz, throwable, JayunLogData.error, message);
    }

    public static void error(Class<?> clazz, Throwable throwable) {
        JayunLogParser.analyze(clazz, throwable, JayunLogData.error, null);
    }

    private static void analyze(Class<?> clazz, Throwable throwable, String level, String message) {
        executor.submit(() -> {
            // 异常类为空不采集
            if (throwable == null)
                return;
            // 数据类异常不采集
            if (throwable instanceof DataException)
                return;
            // 日志不配置不采集
            if (Utils.isEmpty(JayunLogParser.loggerName()))
                return;

            String error = message;
            String fullname = clazz.getName();
            if (Utils.isEmpty(error))
                error = throwable.getMessage();
            Builder builder = new JayunLogData.Builder(fullname, level, error);

            // 读取起源类修改人
            LastModified modified = clazz.getAnnotation(LastModified.class);
            if (modified != null) {
                builder.name(modified.name());
                builder.date(modified.date());
            }

            String[] stack = DefaultThrowableRenderer.render(throwable);
            // 起源类没有在通缉名单上再抓堆栈信息
            if (wanted.stream().noneMatch(fullname::contains)) {
                for (String line : stack) {
                    // 如果捕捉到业务代码就重置触发器信息
                    if (wanted.stream().anyMatch(line::contains)) {
                        line = line.trim();
                        String trigger = JayunLogParser.trigger(line);
                        if (Utils.isEmpty(trigger))
                            continue;
                        builder.id(trigger);
                        builder.line(JayunLogParser.lineNumber(line));

                        try {
                            Class<?> caller = Class.forName(trigger);
                            // 读取通缉令修改人
                            modified = caller.getAnnotation(LastModified.class);
                            if (modified != null) {
                                builder.name(modified.name());
                                builder.date(modified.date());
                            }
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                }
            }

            builder.stack(stack);
            JayunLogData data = builder.build();
            // 推送数据到日志监控队列
            new QueueJayunLog().push(data);
        });
    }

    public static String trigger(String line) {
        // 定义正则表达式模式来匹配类的包名
        Pattern pattern = Pattern.compile("at\\s+([\\w.]+)\\..+");
        Matcher matcher = pattern.matcher(line);
        // 查找匹配项
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null; // 如果没有匹配项，则返回null
    }

    public static String lineNumber(String line) {
        String lineNumber = "?";
        int iend = line.lastIndexOf(')');
        int ibegin = line.lastIndexOf(':', iend - 1);
        if (ibegin == -1)
            return lineNumber;
        return line.substring(ibegin + 1, iend);
    }

    public static void close() {
        executor.shutdownNow();
    }

    public static void main(String[] args) {
        String line = "at site.diteng.start.login.WebDefault.execute(WebDefault.java:121)";
        System.out.println(trigger(line));
        System.out.println(lineNumber(line));
    }

}
