package cn.cerc.core;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

@Deprecated
public class TDateTime extends Datetime {
    private static final long serialVersionUID = -5291970269746288307L;

    public TDateTime() {
        super(StartPoint);
    }

    public TDateTime(Date time) {
        super(time.getTime());
    }

    public TDateTime(long date) {
        super(date);
    }

    public TDateTime(String dateValue) {
        super(dateValue);
    }

    public TDateTime(String fmt, String dateValue) {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        try {
            setTimestamp(sdf.parse(dateValue).getTime());
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public static TDateTime now() {
        return new TDateTime(System.currentTimeMillis());
    }

    @Deprecated
    public static final TDateTime Now() {
        return new TDateTime(System.currentTimeMillis());
    }

    public final TDateTime incSecond(int offset) {
        inc(DateType.Second, offset);
        return this;
    }

    public final TDateTime incMinute(int offset) {
        inc(DateType.Minute, offset);
        return this;
    }

    public final TDateTime incHour(int offset) {
        inc(DateType.Hour, offset);
        return this;
    }

    @Override
    public TDateTime incDay(int offset) {
        inc(DateType.Day, offset);
        return this;
    }

    @Override
    public TDateTime incMonth(int offset) {
        TDateTime result = this.clone();
        result.inc(DateType.Month, offset);
        return result;
    }

    // 返回value的月值 1-12
    public final int getMonth() {
        return get(DateType.Month);
    }

    // 返回value的日值
    @Override
    public final int getDay() {
        return get(DateType.Day);
    }

    // 返回value的小时值
    public final int getHours() {
        return get(DateType.Hour);
    }

    // 返回value的分钟值
    public final int getMinutes() {
        return get(DateType.Minute);
    }

    public final TDateTime monthBof() {
        Datetime result = this.toMonthBof();
        return new TDateTime(result.getTimestamp());
    }

    public final TDateTime monthEof() {
        Datetime result = this.toMonthEof();
        return new TDateTime(result.getTimestamp());
    }

    public static TDateTime fromDate(String dateValue) {
        TDateTime result = new TDateTime(dateValue);
        if (result.isEmpty())
            return null;
        return result;
    }

    @Deprecated
    public static final TDateTime fromYearMonth(String val) {
        return new TDateTime(val);
    }

    /**
     * 获取指定日期的开始时间
     * 
     * @param dateTime 指定时间
     * 
     * @return 开始时间
     */
    public static final TDateTime getStartOfDay(Datetime dateTime) {
        LocalDateTime ldt = dateTime.asLocalDateTime().with(LocalTime.MIN);
        return new TDateTime(ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }

    /**
     * 获取指定日期的结束时刻
     * 
     * @param dateTime 指定时间
     * 
     * @return 结束时间
     */
    public static final TDateTime getEndOfDay(Datetime dateTime) {
        LocalDateTime ldt = dateTime.asLocalDateTime().with(LocalTime.MAX);
        return new TDateTime(ldt.atZone(LocalZone).toInstant().toEpochMilli());
    }

    /**
     * @return 返回当前时间对应英文格式
     */
    public final String getEnDate() {
        SimpleDateFormat sdf = new SimpleDateFormat("EEEEE, MMM dd, yyyy HH:mm:ss a ", Locale.US);
        return sdf.format(this.asBaseDate());
    }

    @Deprecated
    public final boolean isNull() {
        return this.isEmpty();
    }

    @Deprecated
    public String getShortValue() {
        String year = this.getYearMonth().substring(2, 4);
        int month = this.getMonth();
        int day = this.getDay();
        if (TDateTime.now().subtract(DateType.Year, this) != 0) {
            return String.format("%s年%d月", year, month);
        } else {
            return String.format("%d月%d日", month, day);
        }
    }

    @Deprecated
    public final Datetime addDay(int value) {
        return this.incDay(value);
    }

    @Deprecated
    public final TDate asDate() {
        return new TDate(this.getTimestamp());
    }

    /**
     * 是否在指定时间范围内
     *
     * @param start 起始时间段
     * @param last  截止时间段
     * @return 是否在指定时间范围内
     */
    public static final boolean isInterval(String start, String last) {
        Datetime begin = new Datetime(start);
        Datetime end = new Datetime(last);
        Datetime cur = new Datetime(new Datetime().getTime());
        return begin.getTimestamp() <= cur.getTimestamp() && cur.getTimestamp() <= end.getTimestamp();
    }

    public static TDateTime StrToDate(String dateValue) throws ParseException {
        TDateTime result = new TDateTime(dateValue);
        if (result.isEmpty())
            throw new ParseException(String.format("date format error, value=%s", dateValue), 0);
        return result;
    }

    @Deprecated
    public static final String FormatDateTime(String fmt, Datetime value) {
        Map<String, String> map = new HashMap<>();
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
        return value.format(map.get(fmt));
    }

    @Deprecated
    public static final String FormatDateTime(String fmt, Date value) {
        return new Datetime(value).format(fmt);
    }

    /**
     * 计算时间是否到期(精确到秒)
     *
     * @param deadTime    起始时间
     * @param currentTime 截止时间
     * @return 若 deadTime < currentTime 返回 true;
     */
    public static final boolean isTimeOut(Datetime deadTime, Datetime currentTime) {
        return deadTime.before(currentTime);
    }

    public final Date getData() {
        return this.asBaseDate();
    }

    public final TDateTime setData(Date date) {
        if (date == null)
            throw new RuntimeException("data is null");
        setTimestamp(date.getTime());
        return this;
    }

    public final long compareSecond(Datetime from) {
        // 一秒的毫秒数 1000
        return subtract(DateType.Second, from);
    }

    public final long compareMinute(Datetime from) {
        // 一分钟的毫秒数 1000 * 60
        return subtract(DateType.Minute, from);
    }

    public final long compareHour(Datetime from) {
        // 一小时的毫秒数 1000 * 60 * 60
        return subtract(DateType.Hour, from);
    }

    // 若当前值大，则返回正数，否则返回负数
    // 返回this - to 的差异天数 ,返回相对值
    @Override
    public final int compareDay(Datetime from) {
        return this.subtract(DateType.Day, from);
    }

    // 原MonthsBetween，改名为：compareMonth
    public final int compareMonth(Datetime from) {
        return this.subtract(DateType.Month, from);
    }

    public final int compareYear(Datetime from) {
        return this.subtract(DateType.Year, from);
    }

    @Override
    public final TDateTime clone() {
        TDateTime result = new TDateTime(this.timestamp);
        result.setDateSeparator(dateSeparator);
        this.setEmptyDisplay(displayEmpty);
        result.setDateKind(dateKind);
        result.setOptions(options);
        return result;
    }

    /**
     * @return 获取Unix时间戳，一共10位，秒级
     */
    public final long getUnixTimestamp() {
        return this.timestamp / 1000;
    }

}
