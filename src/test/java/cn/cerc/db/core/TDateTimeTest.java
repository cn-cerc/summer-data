package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Date;

import org.junit.Test;

import cn.cerc.db.core.Datetime.DateType;

@SuppressWarnings("deprecation")
public class TDateTimeTest {
    private TDateTime obj;

    @Test
    public void test_getYearMonth() throws ParseException {
        String ym = "201512";
        obj = TDateTime.StrToDate(ym);
        String val = obj.getYearMonth();
        assertEquals("年月与日期互转", ym, val);
    }

    @Test
    public void test_incHour() throws ParseException {
        String ym = "201512";
        obj = TDateTime.StrToDate(ym).incHour(-1);
        obj.setOptions(Datetime.yyyyMMdd_HHmmss);
        assertEquals("减1小时", obj.toString(), "2015-11-30 23:00:00");
        obj = TDateTime.StrToDate(ym).incHour(-25);
        obj.setOptions(Datetime.yyyyMMdd_HHmmss);
        assertEquals("减25小时", obj.toString(), "2015-11-29 23:00:00");
        obj = TDateTime.StrToDate(ym).incHour(1);
        obj.setOptions(Datetime.yyyyMMdd_HHmmss);
        assertEquals("加1小时", obj.getTime(), "01:00:00");
        obj = TDateTime.StrToDate(ym).incHour(12);
        obj.setOptions(Datetime.yyyyMMdd_HHmmss);
        assertEquals("加12小时", obj.getTime(), "12:00:00");
    }

    @Test
    public void test_incMonth() throws ParseException {
        String ym = "201512";
        obj = TDateTime.StrToDate(ym).incMonth(-1);
        assertEquals("取上月初", obj.getYearMonth(), "201511");

        obj = TDateTime.StrToDate("201503").incMonth(-1);
        assertEquals("测试2月份", obj.getYearMonth(), "201502");

        TDateTime date = TDateTime.StrToDate("2016-02-28 08:00:01");
//        assertEquals(28, date.getDay());
        assertEquals(1456617601000L, date.getTimestamp());
//        assertEquals(1456617601, date.getUnixTimestamp());
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
        assertEquals("2016-06-01", date2.incMonth(1).monthBof().toString());
    }

    @Test
    public void test_monthEof() throws ParseException {
        String ym = "201512";
        obj = TDateTime.StrToDate(ym).monthEof();
        assertEquals("取上月末", obj.getDate(), "2015-12-31");
    }

    @Test
    public void test_compareDay() {
        obj = TDateTime.now();
        assertSame(obj.subtract(DateType.Day, TDateTime.now().incDay(-1)), 1);
    }

    @Test
    public void test_FormatDateTime() {
        String val = new TDateTime("2016-01-01").format("yyMMdd");
        assertEquals("160101", val);
    }

    @Test
    public void test_isInterval() {
        assertTrue("范围内", TDateTime.isInterval("05:30", "23:00"));
    }

    @Test
    public void test_isEmpty() {
        assertTrue(new TDateTime().isEmpty());
    }

    @Test
    public void test_empty1() {
        TDateTime date = new TDateTime("");
        assertTrue(date.isEmpty());
        assertEquals("", date.toString());
        assertEquals("0001-01-01", date.getDate());
        assertEquals("000101", date.getYearMonth());
        assertEquals("0001", date.getYear());
//        assertEquals(1, date.getDay());
        assertEquals(0, date.getHours());
        assertEquals(0, date.getMinutes());
    }

    @Test
    public void test_empty2() {
        TDateTime date = new TDateTime();
        date.setEmptyDisplay(true);
        assertTrue(date.isEmpty());
        assertEquals("0001-01-01", date.toString());
        assertEquals("0001-01-01", date.getDate());
        assertEquals("000101", date.getYearMonth());
        assertEquals("0001", date.getYear());
//        assertEquals(1, date.getDay());
        assertEquals(0, date.getHours());
        assertEquals(0, date.getMinutes());
    }

    @Test
    public void test_empty3() {
        TDateTime date = new TDateTime("0000-00-00");
        assertTrue(date.isEmpty());
        date.setEmptyDisplay(true).setOptions(Datetime.yyyyMMdd_HHmmss);
        assertEquals("0001-01-01 00:00:00", date.toString());
        assertEquals("0001-01-01", date.getDate());
        assertEquals("000101", date.getYearMonth());
        assertEquals("0001", date.getYear());
//        assertEquals(1, date.getDay());
        assertEquals(0, date.getHours());
        assertEquals(0, date.getMinutes());
    }

    @Test
    public void test_init() {
        assertEquals("", new TDateTime().toString());
        assertEquals("1970-01-01 08:00:00", new Datetime(0).toString());
        assertEquals("1970-01-01 08:00:00", new Datetime().setTimestamp(0).toString());
        assertEquals("1970-01-01 08:00:00", TDateTime.now().setTimestamp(new Date(0).getTime()).toString());

        assertEquals("", new Datetime(Datetime.StartPoint).toString());
        assertEquals("", new Datetime().setTimestamp(Datetime.StartPoint).toString());
        assertEquals("", TDateTime.now().setTimestamp(new Date(Datetime.StartPoint).getTime()).toString());
    }

    @Test
    public void test_incMonth1() throws ParseException {
        TDateTime obj = TDateTime.StrToDate("201601");
        for (int i = 1; i < 365; i++) {
            check(obj.toString(), i);
        }
        for (int i = 365; i >= 0; i--) {
            check(obj.toString(), i);
        }
    }

    private void check(String base, int offset) {
        TDateTime obj = new TDateTime(base);
        TDateTime val = obj.incMonth(offset);
        int v1 = Integer.parseInt(val.getYearMonth().substring(0, 4)) * 12
                + Integer.parseInt(val.getYearMonth().substring(4, 6));
        int v2 = Integer.parseInt(obj.getYearMonth().substring(0, 4)) * 12
                + Integer.parseInt(obj.getYearMonth().substring(4, 6));
        assertEquals(v1 - v2, offset);
    }

    @Test
    public void test_timeOut() {
        TDateTime dead = TDateTime.now().incDay(-1);
        assertTrue(TDateTime.isTimeOut(dead, TDateTime.now()));

        TDateTime deadTime = new TDateTime("2021/09/05");
        TDateTime current = new TDateTime("2021/09/06");
        assertFalse(deadTime.getData().after(current.getData()));
        assertTrue(TDateTime.isTimeOut(deadTime, current));
        assertTrue(deadTime.before(new Datetime()));
    }
}
