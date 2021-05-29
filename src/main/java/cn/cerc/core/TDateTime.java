package cn.cerc.core;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TDateTime implements Serializable, Comparable<TDateTime>, Cloneable {
    private static final Logger log = LoggerFactory.getLogger(TDateTime.class);
    private static final long serialVersionUID = -7395748632907604015L;
    private static final Map<String, String> dateFormats = new HashMap<>();
    private static final Map<String, String> map;
    private static final ClassResource res = new ClassResource(TDateTime.class, SummerCore.ID);

    static {
        map = new HashMap<>();
        map.put("YYYYMMDD", "yyyyMMdd");
        map.put("YYMMDD", "yyMMdd");
        map.put("YYYMMDD_HH_MM_DD", "yyyyMMdd_HH_mm_dd");
        map.put("yymmddhhmmss", "yyMMddHHmmss");
        map.put("yyyymmdd", "yyyyMMdd");
        map.put("YYYYMMDDHHMMSSZZZ", "yyyyMMddHHmmssSSS");
        map.put("YYYYMM", "yyyyMM");
        map.put("YYYY-MM-DD", "yyyy-MM-dd");
        map.put("yyyy-MM-dd", "yyyy-MM-dd");
        map.put("yyyyMMdd", "yyyyMMdd");
        map.put("YY", "yy");
        map.put("yy", "yy");
        map.put("YYYY", "yyyy");
        map.put("YYYY/MM/DD", "yyyy/MM/dd");

        dateFormats.put("yyyy-MM-dd HH:mm:ss", "\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}");
        dateFormats.put("yyyy-MM-dd HH:mm", "\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}");
        dateFormats.put("yyyy-MM-dd", "\\d{4}-\\d{2}-\\d{2}");
        dateFormats.put("yyyy/MM/dd HH:mm:ss", "\\d{4}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}:\\d{2}");
        dateFormats.put("yyyy/MM/dd HH:mm", "\\d{4}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}");
        dateFormats.put("yyyy/MM/dd", "\\d{4}/\\d{2}/\\d{2}");
        dateFormats.put("yyyyMMdd", "\\d{8}");
        dateFormats.put("yyyyMM", "\\d{6}");
        dateFormats.put("yyyy", "\\d{4}");
    }

    private Date data;

    public TDateTime() {
        this.data = new Date(0);
    }

    public TDateTime(Date value) {
        data = value;
    }

    public TDateTime(String dateValue) {
        super();
        this.data = new Date(0);
        String fmt = getFormat(dateValue);
        if (fmt != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(fmt);
            try {
                this.data = sdf.parse(dateValue);
            } catch (ParseException e) {
                log.error(e.getMessage(), e);
            }
        } else {
            if (!Utils.isEmpty(dateValue)) {
                log.warn("dateValue format error: {}", dateValue);
            }
        }
    }

    public TDateTime(String fmt, String value) {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        try {
            data = sdf.parse(value);
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    // 当时，带时分秒
    public static final TDateTime now() {
        return new TDateTime(new Date());
    }

    @Deprecated
    public static final TDateTime Now() {
        return TDateTime.now();
    }

    /**
     * @deprecated {@code TDateTime.StrToDate}
     */
    @Deprecated
    public static final TDateTime fromYearMonth(String val) {
        TDateTime result = new TDateTime(val);
        if (result.isEmpty())
            throw new RuntimeException(String.format(res.getString(1, "不是 %s 标准年月格式 ：yyyyMM"), val));
        return result;
    }

    private static final String getFormat(String val) {
        if (val == null) {
            return null;
        }
        if ("".equals(val)) {
            return null;
        }
        String fmt = null;
        java.util.Iterator<String> it = dateFormats.keySet().iterator();
        while (it.hasNext() && fmt == null) {
            String key = it.next();
            String str = dateFormats.get(key);
            if (val.matches(str)) {
                fmt = key;
            }
        }
        return fmt;
    }

    /**
     * 计算时间是否到期(精确到秒)
     *
     * @param startTime 起始时间
     * @param endTime   截止时间
     * @return 是否超时
     */
    public static final boolean isTimeOut(TDateTime startTime, TDateTime endTime) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // 一天的毫秒数
        long nd = 1000 * 24 * 60 * 60;

        // 一小时的毫秒数
        long nh = 1000 * 60 * 60;

        // 一分钟的毫秒数
        long nm = 1000 * 60;

        // 一秒钟的毫秒数
        long ns = 1000;

        long diff;
        long day = 0;
        long hour = 0;
        long min = 0;
        long sec = 0;
        try {
            // 计算时间差
            diff = dateFormat.parse(endTime.toString()).getTime() - dateFormat.parse(startTime.toString()).getTime();
            day = diff / nd;// 计算差多少天
            hour = diff % nd / nh + day * 24;// 计算差多少小时
            min = diff % nd % nh / nm + day * 24 * 60;// 计算差多少分钟
            sec = diff % nd % nh % nm / ns;// 计算差多少秒
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // 天数
        if (day > 0) {
            return true;
        }

        // 小时
        if (hour - day * 24 > 0) {
            return true;
        }

        // 分
        if (min - day * 24 * 60 > 0) {
            return true;
        }

        // 秒
        return sec - day > 0;
    }

    @Deprecated
    public static final String FormatDateTime(String fmt, TDateTime value) {
        return value.format(map.get(fmt));
    }

    @Deprecated
    public static final String FormatDateTime(String fmt, Date value) {
        return new TDateTime(value).format(fmt);
    }

    /**
     * 是否在指定时间范围内
     *
     * @param start 起始时间段
     * @param last  截止时间段
     * @return 是否在指定时间范围内
     */
    public static final boolean isInterval(String start, String last) {
        SimpleDateFormat df = new SimpleDateFormat("HH:mm");
        Date now = null;
        Date beginTime = null;
        Date endTime = null;
        try {
            now = df.parse(df.format(new Date()));
            beginTime = df.parse(start);
            endTime = df.parse(last);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Calendar date = Calendar.getInstance();
        date.setTime(now);

        Calendar begin = Calendar.getInstance();
        begin.setTime(beginTime);

        Calendar end = Calendar.getInstance();
        end.setTime(endTime);

        return date.after(begin) && date.before(end);
    }

    @Override
    public String toString() {
        return format("yyyy-MM-dd HH:mm:ss");
    }

    public final String getDate() {
        return format("yyyy-MM-dd");
    }

    public final String getTime() {
        return format("HH:mm:ss");
    }

    /**
     * @return 获取Java时间戳，一共13位，毫秒级
     */
    public final long getTimestamp() {
        return this.getData().getTime();
    }

    /**
     * @return 获取Unix时间戳，一共10位，秒级
     */
    public final long getUnixTimestamp() {
        return this.getData().getTime() / 1000;
    }

    public final String getYearMonth() {
        return format("yyyyMM");
    }

    public final String getMonthDay() {
        return format("MM-dd");
    }

    public final String getYear() {
        return format("yyyy");
    }

    public final String getFull() {
        return format("yyyy-MM-dd HH:mm:ss:SSS");
    }

    public final String format(String fmt) {
        if (isEmpty())
            return "";
        else
            return new SimpleDateFormat(fmt).format(data);
    }

    public final Date getData() {
        return data;
    }

    public final void setData(Date data) {
        if (data == null)
            throw new RuntimeException("data is null");
        this.data = data;
    }

    public final long compareSecond(TDateTime startTime) {
        if (startTime == null) {
            return 0;
        }

        // 一秒的毫秒数
        long second = 1000;

        long start = startTime.getData().getTime();
        long end = TDateTime.now().getData().getTime();
        return (end - start) / second;
    }

    public final long compareMinute(TDateTime startTime) {
        if (startTime == null) {
            return 0;
        }

        // 一分钟的毫秒数
        long minute = 1000 * 60;

        long start = startTime.getData().getTime();
        long end = TDateTime.now().getData().getTime();
        return (end - start) / minute;
    }

    public final long compareHour(TDateTime startTime) {
        if (startTime == null) {
            return 0;
        }

        // 一小时的毫秒数
        long hour = 1000 * 60 * 60;

        long start = startTime.getData().getTime();
        long end = TDateTime.now().getData().getTime();
        return (end - start) / hour;
    }

    // 若当前值大，则返回正数，否则返回负数

    public final int compareDay(TDateTime dateFrom) {
        if (dateFrom == null) {
            return 0;
        }
        // 返回this - to 的差异天数 ,返回相对值
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal1 = Calendar.getInstance();
        Calendar cal2 = Calendar.getInstance();
        String str1 = sdf.format(this.getData());
        String str2 = sdf.format(dateFrom.getData());
        int count = 0;
        try {
            cal1.setTime(sdf.parse(str2));
            cal2.setTime(sdf.parse(str1));
            int flag = 1;
            if (cal1.after(cal2)) {
                flag = -1;
            }
            while (cal1.compareTo(cal2) != 0) {
                cal1.set(Calendar.DAY_OF_YEAR, cal1.get(Calendar.DAY_OF_YEAR) + flag);
                count = count + flag;
            }
        } catch (ParseException e) {
            throw new RuntimeException(String.format(res.getString(4, "日期转换格式错误 ：%s"), e.getMessage()));
        }
        return count;
    }
    // 原MonthsBetween，改名为：compareMonth

    public final int compareMonth(TDateTime dateFrom) {
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(this.getData());
        int month1 = cal1.get(Calendar.YEAR) * 12 + cal1.get(Calendar.MONTH);

        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(dateFrom.getData());
        int month2 = cal2.get(Calendar.YEAR) * 12 + cal2.get(Calendar.MONTH);

        return month1 - month2;
    }

    public final int compareYear(TDateTime dateFrom) {
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(this.getData());
        int year1 = cal1.get(Calendar.YEAR);

        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(dateFrom.getData());
        int year2 = cal2.get(Calendar.YEAR);

        return year1 - year2;
    }

    public final TDate asDate() {
        return new TDate(this.data);
    }

    public final TDateTime incSecond(int value) {
        TDateTime result = this.clone();
        Calendar cal = Calendar.getInstance();
        cal.setTime(result.getData());
        cal.set(Calendar.SECOND, value + cal.get(Calendar.SECOND));
        result.setData(cal.getTime());
        return result;
    }

    public final TDateTime incMinute(int value) {
        TDateTime result = this.clone();
        Calendar cal = Calendar.getInstance();
        cal.setTime(result.getData());
        cal.set(Calendar.MINUTE, value + cal.get(Calendar.MINUTE));
        result.setData(cal.getTime());
        return result;
    }

    public final TDateTime incHour(int value) {
        TDateTime result = this.clone();
        Calendar cal = Calendar.getInstance();
        cal.setTime(result.getData());
        cal.set(Calendar.HOUR_OF_DAY, value + cal.get(Calendar.HOUR_OF_DAY));
        result.setData(cal.getTime());
        return result;
    }

    public final TDateTime incDay(int value) {
        TDateTime result = this.clone();
        Calendar cal = Calendar.getInstance();
        cal.setTime(result.getData());
        cal.set(Calendar.DAY_OF_MONTH, value + cal.get(Calendar.DAY_OF_MONTH));
        result.setData(cal.getTime());
        return result;
    }

    public final TDateTime incMonth(int offset) {
        TDateTime result = this.clone();
        if (offset == 0) {
            return result;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(result.getData());
        int day = cal.get(Calendar.DATE);
        cal.set(Calendar.DATE, 1);
        boolean isMaxDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH) == day;
        cal.set(Calendar.MONTH, cal.get(Calendar.MONTH) + offset);
        int maxDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
        if (isMaxDay || day > maxDay) {
            cal.set(Calendar.DATE, maxDay);
        } else {
            cal.set(Calendar.DATE, day);
        }
        result.setData(cal.getTime());
        return result;
    }

    @Deprecated
    public final TDateTime addDay(int value) {
        return this.incDay(value);
    }

    // 返回value的当月第1天

    public final TDateTime monthBof() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(this.getData());
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        TDateTime tdt = new TDateTime();
        tdt.setData(cal.getTime());
        return tdt;
    }

    public final TDateTime monthEof() {
        // 返回value的当月最后1天
        Calendar cal = Calendar.getInstance();
        cal.setTime(this.getData());
        cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        TDateTime tdt = new TDateTime();
        tdt.setData(cal.getTime());
        return tdt;
    }

    public final int getMonth() {
        // 返回value的月值 1-12
        Calendar cal = Calendar.getInstance();
        cal.setTime(this.data);
        return cal.get(Calendar.MONTH) + 1;
    }

    public final int getDay() {
        // 返回value的日值
        Calendar cal = Calendar.getInstance();
        cal.setTime(this.data);
        return cal.get(Calendar.DAY_OF_MONTH);
    }

    public final int getHours() {
        // 返回value的小时值
        Calendar cal = Calendar.getInstance();
        cal.setTime(this.data);
        return cal.get(Calendar.HOUR_OF_DAY);
    }

    /**
     * 获取指定日期的开始时间
     * 
     * @param dateTime 指定时间
     * 
     * @return 开始时间
     */
    public static final TDateTime getStartOfDay(TDateTime dateTime) {
        Date date = dateTime.getData();
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()),
                ZoneId.systemDefault());
        LocalDateTime startOfDay = localDateTime.with(LocalTime.MIN);
        Date start = Date.from(startOfDay.atZone(ZoneId.systemDefault()).toInstant());
        return new TDateTime(start);
    }

    /**
     * 获取指定日期的结束时刻
     * 
     * @param dateTime 指定时间
     * 
     * @return 结束时间
     */
    public static final TDateTime getEndOfDay(TDateTime dateTime) {
        Date date = dateTime.getData();
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()),
                ZoneId.systemDefault());
        LocalDateTime endOfDay = localDateTime.with(LocalTime.MAX);
        Date end = Date.from(endOfDay.atZone(ZoneId.systemDefault()).toInstant());
        return new TDateTime(end);
    }

    public final int getMinutes() {
        // 返回value的分钟值
        Calendar cal = Calendar.getInstance();
        cal.setTime(this.data);
        return cal.get(Calendar.MINUTE);
    }

    // 返回农历日期
    public final String getGregDate() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(this.getData());
        Lunar lunar = new Lunar(cal);
        return lunar.toString().substring(5).replaceAll("-", "/");
    }

    /**
     * @return 返回当前时间对应英文格式
     */
    public final String getEnDate() {
        SimpleDateFormat sdf = new SimpleDateFormat("EEEEE, MMM dd, yyyy HH:mm:ss a ", Locale.US);
        return sdf.format(data);
    }

    @Override
    public final int compareTo(TDateTime tdt) {
        if (tdt == null) {
            return 1;
        }
        if (tdt.getData().getTime() == this.getData().getTime()) {
            return 0;
        } else {
            return this.getData().getTime() > tdt.getData().getTime() ? 1 : -1;
        }
    }

    @Override
    public final TDateTime clone() {
        return new TDateTime(this.getData());
    }

    @Deprecated
    public final boolean isNull() {
        return this.data == null;
    }

    public final boolean isEmpty() {
        return this.data == null || this.data.getTime() == 0;
    }

    public static final TDateTime StrToDate(String dateValue) throws ParseException {
        TDateTime result = new TDateTime(dateValue);
        if (result.isEmpty())
            throw new ParseException(String.format(res.getString(3, "时间格式不正确: value=%s"), dateValue), 0);
        return result;
    }

    @Deprecated
    public static final TDateTime fromDate(String dateValue) {
        TDateTime result = new TDateTime(dateValue);
        if (result.isEmpty())
            return null;
        return result;
    }

    public static void main(String[] args) {

    }

}
