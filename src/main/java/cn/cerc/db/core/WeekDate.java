package cn.cerc.db.core;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.WeekFields;

/**
 * 星期工具类
 */
public class WeekDate {

    /**
     * 日期在年份的周数
     */
    public static int weekOfYear(Datetime date) {
        LocalDate localDate = date.asLocalDate();
        WeekFields weekFields = WeekFields.ISO;
        int weekNumber = localDate.get(weekFields.weekOfYear()) + 1;
        return weekNumber;
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

}
