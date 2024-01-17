package cn.cerc.db.core;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.WeekFields;
import java.util.Arrays;
import java.util.Locale;

/**
 * 星期工具类
 */
public class WeekDate {

    private static final String[] WEEK_STRINGS = { "周一", "周二", "周三", "周四", "周五", "周六", "周日" };

    /**
     * 日期在年份的周数
     */
    public static int weekOfYear(Datetime date) {
        LocalDate localDate = date.asLocalDate();
        WeekFields weekFields = WeekFields.of(Locale.CHINA);
        return localDate.get(weekFields.weekOfYear());
    }

    /**
     * 周一，每周的第一天
     */
    public static Datetime firstDayOfWeek(Datetime date) {
        LocalDate givenDate = date.asLocalDate();
        LocalDate from = givenDate.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
        return new Datetime(from);
    }

    /**
     * 周日，每周的最后一天
     */
    public static Datetime lastDayOfWeek(Datetime date) {
        LocalDate givenDate = date.asLocalDate();
        LocalDate to = givenDate.with(TemporalAdjusters.nextOrSame(DayOfWeek.SUNDAY));
        return new Datetime(to);
    }

    public static String getWeekString(Datetime date) {
        return getWeekString(date, WEEK_STRINGS);
    }

    public static String getWeekString(Datetime date, String[] weekStrings) {
        if (Utils.isEmpty(weekStrings) || weekStrings.length != 7)
            throw new RuntimeException(String.format("WeekStrings %s 长度不足七位", Arrays.toString(weekStrings)));
        LocalDate givenDate = date.asLocalDate();
        return weekStrings[givenDate.getDayOfWeek().ordinal()];
    }

}
