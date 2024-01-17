package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class WeekDateTest {

    @Test
    public void test_WeekOfYear() {
        Datetime date1 = new Datetime("2023-01-01");
        assertEquals(1, WeekDate.weekOfYear(date1));

        Datetime date2 = new Datetime("2024-01-08");
        assertEquals(2, WeekDate.weekOfYear(date2));

        Datetime date3 = new Datetime("2024-10-16");
        assertEquals(42, WeekDate.weekOfYear(date3));

        Datetime date4 = new Datetime("2024-01-01");
        assertEquals(1, WeekDate.weekOfYear(date4));
    }

    @Test
    public void test_FirstDayOfWeek() {
        Datetime date1 = new Datetime("2023-01-01");
        assertEquals("2022-12-26", WeekDate.firstDayOfWeek(date1).getDate());

        Datetime date2 = new Datetime("2023-10-16");
        assertEquals("2023-10-16", WeekDate.firstDayOfWeek(date2).getDate());

        Datetime date3 = new Datetime("2023-12-31");
        assertEquals("2023-12-25", WeekDate.firstDayOfWeek(date3).getDate());
    }

    @Test
    public void test_LastDayOfWeek() {
        Datetime date1 = new Datetime("2023-01-01");
        assertEquals("2023-01-01", WeekDate.lastDayOfWeek(date1).getDate());

        Datetime date2 = new Datetime("2023-10-16");
        assertEquals("2023-10-22", WeekDate.lastDayOfWeek(date2).getDate());

        Datetime date3 = new Datetime("2023-12-31");
        assertEquals("2023-12-31", WeekDate.lastDayOfWeek(date3).getDate());
    }

    @Test
    public void test_GetWeekString() {
        Datetime date = new Datetime("2024-01-17");
        assertEquals("周三", WeekDate.getWeekString(date));

        String[] weeks = { "一", "二", "三", "四", "五", "六", "日" };
        assertEquals("五", WeekDate.getWeekString(new Datetime("2023-12-01"), weeks));

        String[] err_weeks1 = { "" };
        String[] err_weeks2 = {};
        assertThrows(RuntimeException.class, () -> WeekDate.getWeekString(date, err_weeks1));
        assertThrows(RuntimeException.class, () -> WeekDate.getWeekString(date, err_weeks2));
        assertThrows(RuntimeException.class, () -> WeekDate.getWeekString(date, null));
    }

}
