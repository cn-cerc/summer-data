package cn.cerc.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.LocalDateTime;

import org.junit.Test;

import cn.cerc.core.Datetime.DateType;

public class DatetimeTest {

    @Test
    public void test_null() {
        assertEquals("", new Datetime("").toString());
        assertEquals("", new Datetime("0000-00-00").toString());
        assertEquals("", new Datetime("0000-00-00 00:00:00").toString());
        assertEquals("", new Datetime("0000-00-00 00:00").toString());
        assertEquals("", new Datetime("0001-01-01 00:00:00").toString());
        assertEquals("", new Datetime("0001-01-01 00:00").toString());

        assertEquals("", new Datetime("0000/00/00").toString());
        assertEquals("", new Datetime("0000/00/00 00:00:00").toString());
        assertEquals("", new Datetime("0000/00/00 00:00").toString());
        assertEquals("", new Datetime("0001/01/01 00:00:00").toString());
        assertEquals("", new Datetime("0001/01/01 00:00").toString());

        assertEquals("", new Datetime("000000").toString());
        assertEquals("", new Datetime("0000").toString());
    }

    @Test
    public void test_Options() {
        String text = "2021/09/03 08:23:50";
        Datetime dt = new Datetime(text).setDateSeparator("/");
        assertEquals(dt.toString(), "2021/09/03 08:23:50");

        dt.getOptions().remove(DateType.Second);
        assertEquals(dt.toString(), "2021/09/03 08:23");

        dt.getOptions().removeAll(Datetime.HHmm);
        assertEquals(dt.toString(), "2021/09/03");

        dt.getOptions().remove(DateType.Day);
        assertEquals(dt.toString(), "202109");

        dt.getOptions().remove(DateType.Month);
        assertEquals(dt.toString(), "2021");

        dt.setOptions(Datetime.yyyyMMdd_HHmmss);
        assertEquals(dt.toString(), text);

        dt.getOptions().removeAll(Datetime.yyyyMMdd);
        assertEquals(dt.toString(), "08:23:50");

        dt.getOptions().remove(DateType.Second);
        assertEquals(dt.toString(), "08:23");
    }

    @Test
    public void test_cut() {
        String text = "2021/09/03 08:23:50";
        Datetime dt = new Datetime(text).setDateSeparator("/");
        assertEquals(dt.cut(DateType.Second).toString(), "2021/09/03 08:23:00");
        assertEquals(dt.cut(DateType.Minute).toString(), "2021/09/03 08:00:00");
        assertEquals(dt.cut(DateType.Hour).toString(), "2021/09/03 00:00:00");
        assertEquals(dt.cut(DateType.Day).toString(), "2021/09/01 00:00:00");
        assertEquals(dt.cut(DateType.Month).toString(), "2021/01/01 00:00:00");
        assertEquals(dt.setOptions(Datetime.yyyyMMdd_HHmmss).toString(), "2021/01/01 00:00:00");
    }

    @Test
    public void test_subtract() {
        String text1 = "2020/09/06 08:23:50";// 闰年
        Datetime dt1 = new Datetime(text1).setDateSeparator("/");

        String text2 = "2021/09/06 08:23:50";
        Datetime dt2 = new Datetime(text2).setDateSeparator("/");
        LocalDateTime self = dt2.asLocalDateTime();
        assertEquals(9, self.getMonthValue());// 本年第 ? 月
        assertEquals(249, self.getDayOfYear());// 本年第 ? 天
        assertEquals(6, self.getDayOfMonth());// 本月第 ? 天
        assertEquals(1, dt2.subtract(DateType.Year, dt1));// 差异年数
        assertEquals(12, dt2.subtract(DateType.Month, dt1));// 差异月数
        assertEquals(364, dt2.subtract(DateType.Day, dt1));// 差异日数
        assertEquals(8760, dt2.subtract(DateType.Hour, dt1));// 差异时数
        assertEquals(525600, dt2.subtract(DateType.Minute, dt1));// 差异分数
        assertEquals(31536000, dt2.subtract(DateType.Second, dt1));// 差异秒数
    }

    @Test
    public void test_inc() {
        String text = "2021/09/03 08:23:50";
        Datetime dt = new Datetime(text).setDateSeparator("/");
        dt.inc(DateType.Second, 5);
        assertEquals(dt.toString(), "2021/09/03 08:23:55");
        dt.inc(DateType.Minute, 5);
        assertEquals(dt.toString(), "2021/09/03 08:28:55");
        dt.inc(DateType.Hour, 2);
        assertEquals(dt.toString(), "2021/09/03 10:28:55");
        dt.inc(DateType.Day, 2);
        assertEquals(dt.toString(), "2021/09/05 10:28:55");
        dt.inc(DateType.Month, 2);
        assertEquals(dt.toString(), "2021/11/05 10:28:55");
        dt.inc(DateType.Year, 3);
        assertEquals(dt.toString(), "2024/11/05 10:28:55");
        // 跨年
        dt.inc(DateType.Month, 13);
        assertEquals(dt.toString(), "2025/12/05 10:28:55");
    }

    @Test
    public void test_compareTo() {
        String text = "2021/09/03 08:23:50";
        Datetime dt = new Datetime(text).setDateSeparator("/");
        assertEquals(dt.compareTo(dt), 0);
        assertEquals(dt.compareTo(new Datetime()), -1);
        assertEquals(new Datetime().compareTo(dt), 1);
    }

    @Test
    public void test_isInterval() {
        Datetime begin = new Datetime("08:00");
        Datetime end = new Datetime("09:00");

        assertTrue(begin.before(new Datetime("08:05")));
        assertTrue(end.after(new Datetime("08:05")));
    }

    @Test
    public void test_build() {
        String text = "2021-01-02 23:59:02";
        assertEquals(text, new Datetime(text).toString());
        text = "2021/01/02 23:59:02";
        assertEquals(text, new Datetime(text).toString());
        text = "2021-01-02 23:59:00";
        assertEquals(text, new Datetime(text).toString());
        text = "2021-01-02";
        assertEquals(text, new Datetime(text).toString());
        text = "20210101";
        assertEquals("2021-01-01", new Datetime(text).toString());
        text = "2021";
        assertEquals("2021-01-01", new Datetime(text).toString());
        text = "23:59:02";
        assertEquals(text, new Datetime(text).toString());
        text = "23:59";
        assertEquals("23:59:00", new Datetime(text).toString());
    }

    @Test
    public void test_monthBof() {
        String text = "2021-01-02 23:59:02";
        assertEquals("2021-01-01", new Datetime(text).toMonthBof().toString());
    }

    @Test
    public void test_monthEof() {
        String text = "2021-01-02 23:59:02";
        assertEquals("2021-01-31 23:59:59", new Datetime(text).toMonthEof().toString());
    }

    @Test
    public void test_toStartOfDay() {
        String text = "2021-01-02 23:59:02";
        assertEquals("2021-01-02 00:00:00", new Datetime(text).toDayStart().toString());
    }

    @Test
    public void test_toEndOfDay() {
        String text = "2021-01-02 23:59:02";
        assertEquals("2021-01-02 23:59:59", new Datetime(text).toDayEnd().toString());
    }

    @Test
    public void test_FastTime_FastDate() {
        Datetime datetime = new Datetime("2021-01-02 08:09:02");

        FastDate fastDate = datetime.toFastDate();
        assertEquals("2021-01-02", fastDate.toString());
        assertEquals("2021-01-02 00:00:00:000", fastDate.getFull());

        datetime = datetime.toDayEnd();
        assertEquals("2021-01-02 23:59:59:999", datetime.getFull());

        FastTime fastTime = datetime.toFastTime();
        assertEquals("23:59:59", fastTime.toString());
        assertEquals("0001-01-01 23:59:59:999", fastTime.getFull());

        assertEquals("2021-01-02 23:59:59", new Datetime(fastDate, fastTime).toString());
        assertEquals("2021-01-31 23:59:59", datetime.toMonthEof().toString());
    }

    @Test
    public void test_toJson() {
        FieldDefs def = new FieldDefs();
        Record item = new Record(def);
        String jsonStr = "{\"Boolean\":true," + "\"Date\":\"2016-06-20 00:00:00\","
                + "\"DateTime\":\"2016-06-20 09:26:35\"," + "\"Double\":3.12," + "\"Integer\":123," + "\"Null\":null,"
                + "\"OldDate\":\"2016-06-20 09:26:35\"," + "\"String\":\"AAA\"}";

        item.setField("String", "AAA");
        item.setField("Double", 3.12);
        item.setField("Integer", 123);
        item.setField("OldDate", "2016-06-20 09:26:35");
        item.setField("Date", "2016-06-20 00:00:00");
        item.setField("DateTime", "2016-06-20 09:26:35");
        item.setField("Boolean", true);
        item.setField("Null", null);
        assertEquals(jsonStr, item.toString());

        item.setJSON(jsonStr);
        assertEquals("AAA", item.getString("String"));
        assertEquals(jsonStr, item.toString());
    }

}
