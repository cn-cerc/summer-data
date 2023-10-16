package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class WeekDateTest {

    @Test
    public void test_WeekOfYear() {
        Datetime date1 = new Datetime("2023-01-01");
        assertEquals(1, WeekDate.weekOfYear(date1));

        Datetime date2 = new Datetime("2023-01-07");
        assertEquals(2, WeekDate.weekOfYear(date2));

        Datetime date3 = new Datetime("2023-10-16");
        assertEquals(43, WeekDate.weekOfYear(date3));
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

}
