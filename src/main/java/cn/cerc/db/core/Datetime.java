package cn.cerc.db.core;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;

import cn.cerc.db.testsql.TestsqlServer;

/**
 * 日期工具类
 */
public class Datetime implements Serializable, Comparable<Datetime>, Cloneable {
    private static final long serialVersionUID = -7395748632907604015L;
    // 常见输出组合
    public static final EnumSet<DateType> yyyyMMdd_HHmmss = EnumSet.of(DateType.Year, DateType.Month, DateType.Day,
            DateType.Hour, DateType.Minute, DateType.Second);
    public static final EnumSet<DateType> yyyyMMdd_HHmm = EnumSet.of(DateType.Year, DateType.Month, DateType.Day,
            DateType.Hour, DateType.Minute);
    public static final EnumSet<DateType> yyyyMMdd = EnumSet.of(DateType.Year, DateType.Month, DateType.Day);
    public static final EnumSet<DateType> yyyyMM = EnumSet.of(DateType.Year, DateType.Month);
    public static final EnumSet<DateType> yyyy = EnumSet.of(DateType.Year);
    public static final EnumSet<DateType> HHmmss = EnumSet.of(DateType.Hour, DateType.Minute, DateType.Second);
    public static final EnumSet<DateType> HHmm = EnumSet.of(DateType.Hour, DateType.Minute);

    public static final long StartPoint = -62135625943000L;
    /**
     * Date 对象在 StartPoint 时间戳下UTC的日期时间
     */
    public static final String dateZero = "0001-01-02 23:54:17";
    /**
     * LocalDateTime 对象在时间戳下标准的显示日期时间
     */
    public static final String localZero = "0001-01-01 00:00:00";
    protected static final ZoneId LocalZone = ZoneId.systemDefault();
    private static final char[] DateCharList = new char[] { ' ', 'T', ':', '/', '-', '0', '1', '2', '3', '4', '5', '6',
            '7', '8', '9' };

    protected DateKind dateKind = DateKind.DateTime;
    protected EnumSet<DateType> options = EnumSet.allOf(DateType.class);
    protected long timestamp;
    protected String dateSeparator = "-";
    protected boolean displayEmpty = false;

    // 字段：年、月、日、时、分、秒
    public enum DateType {
        Year,
        Month,
        Day,
        Hour,
        Minute,
        Second
    }

    public enum DateKind {
        DateTime,
        OnlyDate,
        OnlyTime;
    }

    public Datetime() {
        super();
        var current = System.currentTimeMillis();
        if (TestsqlServer.enabled())
            current = TestsqlServer.build().getLockTime().orElse(current);
        this.setTimestamp(current);
    }

    public Datetime(long timestamp) {
        super();
        this.setTimestamp(timestamp);
    }

    public Datetime(Date date) {
        super();
        this.setTimestamp(date.getTime());
    }

    public Datetime(FastDate fastDate, FastTime fastTime) {
        this(fastDate.getDate() + " " + fastTime.getTime());
    }

    public Datetime(String dateValue) {
        super();

        LocalDateTime ldt;
        try {
            ldt = build(dateValue);
            Instant instant = ldt.atZone(LocalZone).toInstant();
            this.setTimestamp(instant.toEpochMilli());
        } catch (DateFormatErrorException | DateTimeException e) {
            this.timestamp = StartPoint;
        }
    }

    /**
     * LocalDateTime 转 Datetime
     */
    public Datetime(LocalDateTime localDateTime) {
        super();
        try {
            Instant instant = localDateTime.atZone(LocalZone).toInstant();
            this.setTimestamp(instant.toEpochMilli());
        } catch (DateTimeException e) {
            this.timestamp = StartPoint;
        }
    }

    /**
     * LocalDate 转 Datetime
     */
    public Datetime(LocalDate localDate) {
        super();
        try {
            Instant instant = localDate.atStartOfDay(LocalZone).toInstant();
            this.setTimestamp(instant.toEpochMilli());
        } catch (DateTimeException e) {
            this.timestamp = StartPoint;
        }
    }

    @Override
    public String toString() {
        if (isEmpty() && !displayEmpty)
            return "";

        String fmt;
        if (isOptions(yyyyMMdd_HHmmss))
            fmt = String.format("yyyy%sMM%sdd HH:mm:ss", dateSeparator, dateSeparator);
        else if (isOptions(yyyyMMdd_HHmm))
            fmt = String.format("yyyy%sMM%sdd HH:mm", dateSeparator, dateSeparator);
        else if (isOptions(yyyyMMdd))
            fmt = String.format("yyyy%sMM%sdd", dateSeparator, dateSeparator);
        else if (isOptions(yyyyMM))
            fmt = "yyyyMM";
        else if (isOptions(yyyy))
            fmt = "yyyy";
        else if (isOptions(HHmmss))
            fmt = "HH:mm:ss";
        else if (isOptions(HHmm))
            fmt = "HH:mm";
        else
            throw new RuntimeException("output options error: " + options);
        return format(fmt);
    }

    public final String getDate() {
        return format(String.format("yyyy%sMM%sdd", dateSeparator, dateSeparator));
    }

    public final String getTime() {
        return format("HH:mm:ss");
    }

    /**
     * @return 获取Java时间戳，一共13位，毫秒级
     */
    public final long getTimestamp() {
        return this.timestamp;
    }

    public final Datetime setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        if (isDateTime()) {
            Instant instant = Instant.ofEpochMilli(timestamp);
            LocalDateTime ldt = LocalDateTime.ofInstant(instant, LocalZone);
            if (ldt.getHour() == 0 && ldt.getMinute() == 0 && ldt.getSecond() == 0 && ldt.getNano() == 0)
                this.setOptions(yyyyMMdd);
            else if (ldt.getYear() == 1 && ldt.getMonthValue() == 1 && ldt.getDayOfMonth() == 1)
                this.setOptions(HHmmss);
        }
        return this;
    }

    public final String getYearMonth() {
        return format("yyyyMM");
    }

    public final String getMonthDay() {
        return format(String.format("MM%sdd", dateSeparator));
    }

    public final String getYear() {
        return format("yyyy");
    }

    public final String getFull() {
        return format(String.format("yyyy%sMM%sdd HH:mm:ss:SSS", dateSeparator, dateSeparator));
    }

    public final String format(String fmt) {
        DateTimeFormatter format = DateTimeFormatter.ofPattern(fmt);
        LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), LocalZone);
        return ldt.format(format);
    }

    /**
     * this 减 target
     *
     * @param dateType 指定字段
     * @param target   目标
     * @return 返回指定的字段数据差值
     */
    public final int subtract(DateType dateType, Datetime target) {
        if (target == null)
            return 0;

        switch (dateType) {
        case Year: {
            return this.asLocalDateTime().getYear() - target.asLocalDateTime().getYear();
        }
        case Month: {
            LocalDateTime self = this.asLocalDateTime();
            LocalDateTime item = target.asLocalDateTime();
            int year = (self.getYear() - item.getYear()) * 12;
            return year + self.getMonthValue() - item.getMonthValue();
        }
        case Day: {
            LocalDateTime self = this.asLocalDateTime();
            LocalDateTime item = target.asLocalDateTime();
            int year = (self.getYear() - item.getYear()) * 365;
            return year + self.getDayOfYear() - item.getDayOfYear();
        }
        case Hour:
            return (int) ((this.timestamp - target.timestamp) / 1000 / 60 / 60);
        case Minute:
            return (int) ((this.timestamp - target.timestamp) / 1000 / 60);
        case Second:
            return (int) ((this.timestamp - target.timestamp) / 1000);
        default:
            return 0;
        }
    }

    /**
     * 增、减指定的字段数据
     *
     * @param dateType 指定字段
     * @param offset   偏移量，可为正，也可为负
     * @return this 新的对象
     */
    public Datetime inc(DateType dateType, int offset) {
        LocalDateTime ldt = this.asLocalDateTime();
        switch (dateType) {
        case Year:
            ldt = ldt.plusYears(offset);
            break;
        case Month:
            ldt = ldt.plusMonths(offset);
            break;
        case Day:
            ldt = ldt.plusDays(offset);
            break;
        case Hour:
            ldt = ldt.plusHours(offset);
            break;
        case Minute:
            ldt = ldt.plusMinutes(offset);
            break;
        case Second:
            ldt = ldt.plusSeconds(offset);
            break;
        default:
            break;
        }
        Datetime result = this.clone();
        result.setTimestamp(ldt.atZone(LocalZone).toInstant().toEpochMilli());
        return result;
    }

    public final Date asBaseDate() {
        Instant instant = Instant.ofEpochMilli(this.timestamp);
        return Date.from(instant);
    }

    public final LocalDateTime asLocalDateTime() {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), LocalZone);
    }

    public final LocalDate asLocalDate() {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), LocalZone).toLocalDate();
    }

    /**
     * 仅返回日期部分
     *
     * @return 创建新的对象 FastDate
     */
    public final FastDate toFastDate() {
        return new FastDate(this.getTimestamp());
    }

    /**
     * 仅返回时间部分
     *
     * @return 创建新的对象 FastTime
     */
    public final FastTime toFastTime() {
        return new FastTime(this.getTimestamp());
    }

    public final int get(DateType dateType) {
        LocalDateTime ldt = this.asLocalDateTime();
        return switch (dateType) {
        case Year -> ldt.getYear();
        case Month -> ldt.getMonthValue();
        case Day -> ldt.getDayOfMonth();
        case Hour -> ldt.getHour();
        case Minute -> ldt.getMinute();
        case Second -> ldt.getSecond();
        default -> 0;
        };
    }

    // 返回农历日期
    public final String getGregDate() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(this.timestamp);
        Lunar lunar = new Lunar(cal);
        return lunar.toString().substring(5).replaceAll("-", "/");
    }

    @Override
    public final int compareTo(Datetime dateFrom) {
        if (dateFrom == null)
            return 1;

        if (dateFrom.getTimestamp() == this.getTimestamp()) {
            return 0;
        } else {
            return this.after(dateFrom) ? 1 : -1;
        }
    }

    @Override
    public Datetime clone() {
        Datetime result = new Datetime(this.timestamp);
        result.setDateSeparator(dateSeparator);
        this.setEmptyDisplay(displayEmpty);
        result.setDateKind(dateKind);
        result.setOptions(options);
        return result;
    }

    public final boolean isEmpty() {
        return this.timestamp == StartPoint;
    }

    /**
     * 切除指定的字段数据
     *
     * @param dateType 从指定的字段开始切除（年月日设置为1，时分秒设置为0）
     * @return this 当前对象
     */
    public final Datetime cut(DateType dateType) {
        LocalDateTime ldt = this.asLocalDateTime();

        if (dateType == DateType.Year)
            ldt = ldt.withYear(1);

        if (dateType.ordinal() <= DateType.Month.ordinal())
            ldt = ldt.withMonth(1);

        if (dateType.ordinal() <= DateType.Day.ordinal())
            ldt = ldt.withDayOfMonth(1);

        if (dateType.ordinal() <= DateType.Hour.ordinal())
            ldt = ldt.withHour(0);

        if (dateType.ordinal() <= DateType.Minute.ordinal())
            ldt = ldt.withMinute(0);

        if (dateType.ordinal() <= DateType.Second.ordinal())
            ldt = ldt.withSecond(0).withNano(0);

        this.timestamp = ldt.atZone(LocalZone).toInstant().toEpochMilli();
        return this;
    }

    /**
     * 返回起始日期，等同于new Datetime("0001-01-01 00:00:00")
     *
     * @return 返回新的对象
     */
    public static Datetime zero() {
        return new Datetime(StartPoint);
    }

    /**
     * 返回当前时间的当月第1天
     *
     * @return 返回新的对象
     */
    public Datetime toMonthBof() {
        LocalDateTime ldt = this.asLocalDateTime();
        ldt = LocalDateTime.of(ldt.getYear(), ldt.getMonthValue(), 1, 0, 0, 0);
        return new Datetime(ldt.atZone(LocalZone).toInstant().toEpochMilli());
    }

    /**
     * 返回当前时间的当月最后1天
     *
     * @return 返回新的对象
     */
    public final Datetime toMonthEof() {
        LocalDateTime ldt = this.asLocalDateTime();
        ldt = LocalDateTime.of(ldt.getYear(), ldt.getMonthValue(), 1, 0, 0, 0);
        ldt = ldt.plusMonths(1).plusDays(-1);
        return new Datetime(ldt.atZone(LocalZone).toInstant().toEpochMilli()).toDayEnd();
    }

    public boolean isOptions(EnumSet<DateType> target) {
        if (options.size() != target.size())
            return false;
        return options.containsAll(target);
    }

    public final String getDateSeparator() {
        return dateSeparator;
    }

    public final Datetime setDateSeparator(String dateSeparator) {
        this.dateSeparator = dateSeparator;
        return this;
    }

    public final boolean isEmptyDisplay() {
        return displayEmpty;
    }

    public final Datetime setEmptyDisplay(boolean emptyDisplay) {
        this.displayEmpty = emptyDisplay;
        return this;
    }

    private final LocalDateTime build(String text) throws DateFormatErrorException {
        LocalDateTime ldt = LocalDateTime.of(1, 1, 1, 0, 0, 0);
        if (text.length() < 4)
            return ldt;
        // 检查非法字符
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            boolean found = false;
            for (char c : DateCharList) {
                if (ch == c) {
                    found = true;
                    break;
                }
            }
            if (!found)
                throw new DateFormatErrorException(text);
        }
        switch (text.length()) {
        case 4: {
            this.setOptions(yyyyMMdd);
            int year = Integer.parseInt(text);
            return ldt.withYear(year == 0 ? 1 : year);
        }
        case 5: {
            String[] items = text.split(":");
            if (items.length != 2)
                throw new DateFormatErrorException(text);
            this.setOptions(HHmmss);
            return ldt.withHour(Integer.parseInt(items[0])).withMinute(Integer.parseInt(items[1]));
        }
        case 6: {
            this.setOptions(yyyyMMdd);
            int year = Integer.parseInt(text.substring(0, 4));
            int month = Integer.parseInt(text.substring(4, 6));
            return ldt.withYear(year == 0 ? 1 : year).withMonth(month == 0 ? 1 : month);
        }
        case 8: {
            String[] items = text.split(":");
            if (items.length == 3) {
                this.setOptions(HHmmss);
                return ldt.withHour(Integer.parseInt(text.substring(0, 2)))
                        .withMinute(Integer.parseInt(text.substring(3, 5)))
                        .withSecond(Integer.parseInt(text.substring(6, 8)));
            } else {
                this.setOptions(yyyyMMdd);
                return ldt.withYear(Integer.parseInt(text.substring(0, 4)))
                        .withMonth(Integer.parseInt(text.substring(4, 6)))
                        .withDayOfMonth(Integer.parseInt(text.substring(6, 8)));
            }
        }
        case 10: {
            if (text.contains("-")) {
                this.setDateSeparator("-");
            } else if (text.contains("/"))
                this.setDateSeparator("/");
            else
                throw new DateFormatErrorException(text);
            this.setOptions(yyyyMMdd);
            int year = Integer.parseInt(text.substring(0, 4));
            int month = Integer.parseInt(text.substring(5, 7));
            int day = Integer.parseInt(text.substring(8, 10));
            return ldt.withYear(year == 0 ? 1 : year)
                    .withMonth(month == 0 ? 1 : month)
                    .withDayOfMonth(day == 0 ? 1 : day);
        }
        case 16:
        case 19: {
            String date = text.substring(0, 10);
            String time = text.substring(11);
            String date_fmt = null;
            if (date.contains("-")) {
                date_fmt = "yyyy-MM-dd";
                this.setDateSeparator("-");
            } else if (date.contains("/")) {
                this.setDateSeparator("/");
                date_fmt = "yyyy/MM/dd";
            } else
                throw new DateFormatErrorException(text);

            String time_fmt = null;
            switch (time.split(":").length) {
            case 2: {
                this.setOptions(yyyyMMdd_HHmmss);
                time_fmt = "HH:mm";
                break;
            }
            case 3: {
                this.setOptions(yyyyMMdd_HHmmss);
                time_fmt = "HH:mm:ss";
                break;
            }
            default:
                throw new DateFormatErrorException(text);
            }

            String value = text.replace("T", " ");
            // 防止年月日均为0
            if ("0000-00-00".equals(text.substring(0, 10)))
                value = "0001-01-01" + text.substring(10);
            else if ("0000/00/00".equals(text.substring(0, 10)))
                value = "0001/01/01" + text.substring(10);
            DateTimeFormatter df = DateTimeFormatter.ofPattern(date_fmt + ' ' + time_fmt);
            return LocalDateTime.parse(value, df);
        }
        default:
            throw new DateFormatErrorException(text);
        }
    }

    /**
     * 判断当前对象是否在指定对象之后
     *
     * @param target 比较对象
     * @return 当前时间戳 大于 目标时间戳
     */
    public boolean after(Datetime target) {
        return this.getTimestamp() > target.getTimestamp();
    }

    /**
     * 判断当前对象是否在指定对象之前
     *
     * @param target 比较对象
     * @return 当前时间戳 小于 目标时间戳
     */
    public boolean before(Datetime target) {
        return this.getTimestamp() < target.getTimestamp();
    }

    /**
     * 判断当前对象是否在指定的时间区间内
     * 
     * @param start 区间开始
     * @param end   区间结束
     * @return 是否在区间内
     */
    public boolean between(Datetime start, Datetime end) {
        return this.getTimestamp() >= start.getTimestamp() && this.getTimestamp() <= end.getTimestamp();
    }

    /**
     * 获取指定日期的开始时间
     *
     * @return 返回新的对象
     */
    public final Datetime toDayStart() {
        LocalDateTime ldt = this.asLocalDateTime().with(LocalTime.MIN);
        Datetime result = new Datetime(ldt.atZone(LocalZone).toInstant().toEpochMilli());
        return result.setOptions(yyyyMMdd_HHmmss);
    }

    /**
     * 获取指定日期的结束时刻
     *
     * @return 返回新的对象
     */
    public final Datetime toDayEnd() {
        LocalDateTime ldt = this.asLocalDateTime().with(LocalTime.MAX);
        Datetime result = new Datetime(ldt.atZone(LocalZone).toInstant().toEpochMilli());
        return result.setOptions(yyyyMMdd_HHmmss);
    }

    public final DateKind getDateKind() {
        return dateKind;
    }

    public Datetime setDateKind(DateKind dateKind) {
        if (this.dateKind == dateKind)
            return this;
        this.dateKind = dateKind;
        return switch (dateKind) {
        case OnlyDate -> {
            setOptions(yyyyMMdd);
            yield this;
        }
        case OnlyTime -> {
            setOptions(HHmmss);
            yield this;
        }
        default -> this;
        };
    }

    public final boolean isDateTime() {
        return dateKind == DateKind.DateTime;
    }

    public final boolean isOnlyDate() {
        return dateKind == DateKind.OnlyDate;
    }

    public final boolean isOnlyTime() {
        return dateKind == DateKind.OnlyTime;
    }

    public final EnumSet<DateType> getOptions() {
        return options;
    }

    public Datetime setOptions(EnumSet<DateType> options) {
        if (this.isOnlyDate()) {
            if (compareOptions(options, HHmmss) || compareOptions(options, HHmm))
                throw new RuntimeException("exclude options " + options);
        } else if (this.isOnlyTime()) {
            if (!compareOptions(options, HHmmss) && !compareOptions(options, HHmm)
                    && !compareOptions(options, yyyyMMdd_HHmmss) && !compareOptions(options, yyyyMMdd_HHmm))
                throw new RuntimeException("exclude options " + options);
        }
        this.options.clear();
        this.options.addAll(options);
        return this;
    }

    private boolean compareOptions(EnumSet<DateType> source, EnumSet<DateType> target) {
        if (source.size() != target.size())
            return false;
        return source.containsAll(target);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Datetime datetime)
            return this.subtract(DateType.Second, datetime) == 0;
        return super.equals(obj);
    }

//    @Deprecated
//    public Datetime incDay(int offset) {
//        return inc(DateType.Day, offset);
//    }
//
//    @Deprecated
//    public int getDay() {
//        return get(DateType.Day);
//    }
//
//    @Deprecated
//    public int compareDay(Datetime target) {
//        return this.subtract(DateType.Day, target);
//    }
//
//    @Deprecated
//    public Datetime incMonth(int offset) {
//        return inc(DateType.Month, offset);
//    }
//
//    @Deprecated
//    public int compareMonth(Datetime target) {
//        return this.subtract(DateType.Month, target);
//    }

}
