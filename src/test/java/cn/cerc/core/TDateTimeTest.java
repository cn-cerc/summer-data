package cn.cerc.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;

import org.junit.Test;

public class TDateTimeTest {
    private String ym = "201512";
    private TDateTime obj;

    @Test
    public void test_getYearMonth() throws ParseException {
        obj = TDateTime.StrToDate(ym);
        String val = obj.getYearMonth();
        assertEquals("年月与日期互转", ym, val);
    }

    @Test
    public void test_incHour() throws ParseException {
        obj = TDateTime.StrToDate(ym).incHour(-1);
        assertEquals("减1小时", obj.toString(), "2015-11-30 23:00:00");
        obj = TDateTime.StrToDate(ym).incHour(-25);
        assertEquals("减25小时", obj.toString(), "2015-11-29 23:00:00");
        obj = TDateTime.StrToDate(ym).incHour(1);
        assertEquals("加1小时", obj.getTime(), "01:00:00");
        obj = TDateTime.StrToDate(ym).incHour(12);
        assertEquals("加12小时", obj.getTime(), "12:00:00");
    }

    @Test
    public void test_incMonth() throws ParseException {
        obj = TDateTime.StrToDate(ym).incMonth(-1);
        assertEquals("取上月初", obj.getYearMonth(), "201511");

        obj = TDateTime.StrToDate("201503").incMonth(-1);
        assertEquals("测试2月份", obj.getYearMonth(), "201502");

        TDateTime date = TDateTime.StrToDate("2016-02-28 08:00:01");
        assertEquals(28, date.getDay());
        assertEquals(1456617601000L, date.getTimestamp());
        assertEquals(1456617601, date.getUnixTimestamp());
        assertEquals("2016-02-28 08:00:01", date.toString());
        assertEquals("2016-03-28 08:00:01", date.incMonth(1).toString());
        assertEquals("2016-04-28 08:00:01", date.incMonth(2).toString());
        assertEquals("2016-05-28 08:00:01", date.incMonth(3).toString());
        assertEquals("2016-06-28 08:00:01", date.incMonth(4).toString());
        assertEquals("2017-02-28 08:00:01", date.incMonth(12).toString());
        assertEquals("2017-03-28 08:00:01", date.incMonth(13).toString());
        assertEquals("2016-02-28 08:00:01", date.toString());

        TDateTime date2 = TDateTime.StrToDate("2016-05-31 23:59:59");
        assertEquals("2016-05-31 23:59:59", date2.toString());
        assertEquals("2016-06-30 23:59:59", date2.incMonth(1).toString());
        assertEquals("2016-06-01 00:00:00", date2.incMonth(1).monthBof().toString());
    }

    @Test
    public void test_monthEof() throws ParseException {
        obj = TDateTime.StrToDate(ym).monthEof();
        assertEquals("取上月末", obj.getDate(), "2015-12-31");
    }

    @Test
    public void test_compareDay() {
        obj = TDateTime.now();
        assertSame(obj.compareDay(TDateTime.now().incDay(-1)), 1);
    }

    @Test
    public void test_FormatDateTime() {
        String val = new TDateTime("2016-01-01").format("yyMMdd");
        assertEquals("160101", val);
    }

    @Test
    public void test_isInterval() {
        assertTrue("范围内", TDateTime.isInterval("05:30", "17:00"));
    }

    @Test
    public void test_isEmpty() {
        assertTrue(new TDateTime().isEmpty());
    }

    @Test(expected = RuntimeException.class)
    public void test_isNew() {
        new TDateTime("20211").getDate();
    }

    public void test_empty1() {
        TDateTime date = new TDateTime("");
        assertTrue(date.isEmpty());
        assertEquals("", date.toString());
        assertEquals("0000-00-00", date.getDate());
        assertEquals("000000", date.getYearMonth());
        assertEquals("0000", date.getYear());
        assertEquals(0, date.getDay());
        assertEquals(0, date.getHours());
        assertEquals(0, date.getMinutes());
    }

    public void test_empty2() {
        TDateTime date = new TDateTime();
        assertTrue(date.isEmpty());
        assertEquals("0000-00-00 00:00:00", date.toString());
        assertEquals("0000-00-00", date.getDate());
        assertEquals("000000", date.getYearMonth());
        assertEquals("0000", date.getYear());
        assertEquals(0, date.getDay());
        assertEquals(0, date.getHours());
        assertEquals(0, date.getMinutes());
    }

    public void test_empty3() {
        TDateTime date = new TDateTime("0000-00-00");
        assertTrue(date.isEmpty());
        assertEquals("0000-00-00 00:00:00", date.toString());
        assertEquals("0000-00-00", date.getDate());
        assertEquals("000000", date.getYearMonth());
        assertEquals("0000", date.getYear());
        assertEquals(0, date.getDay());
        assertEquals(0, date.getHours());
        assertEquals(0, date.getMinutes());
    }
}
