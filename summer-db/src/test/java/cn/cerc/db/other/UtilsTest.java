package cn.cerc.db.other;

import static cn.cerc.core.Utils.assigned;
import static cn.cerc.core.Utils.ceil;
import static cn.cerc.core.Utils.copy;
import static cn.cerc.core.Utils.floatToStr;
import static cn.cerc.core.Utils.isNumeric;
import static cn.cerc.core.Utils.roundTo;
import static cn.cerc.core.Utils.strToDoubleDef;
import static cn.cerc.core.Utils.trunc;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.text.DecimalFormat;

import org.junit.Test;

import cn.cerc.core.Utils;

public class UtilsTest {

    @Test
    public void test_roundTo_1() {
//        double val = 37121 * 0.475;
        assertEquals(Utils.roundTo(9.8249, -2), 9.82, 0);
        assertEquals(Utils.roundTo(9.82671, -2), 9.83, 0);
        assertEquals(Utils.roundTo(9.8350, -2), 9.84, 0);
        assertEquals(Utils.roundTo(9.8351, -2), 9.84, 0);
        assertEquals(Utils.roundTo(9.8250, -2), 9.82, 0);
        assertEquals(Utils.roundTo(9.82501, -2), 9.83, 0);

        double value = 9.82575d;
        DecimalFormat df = new DecimalFormat("0.####");
        assertEquals(df.format(value), "9.8257");// 丢失精度
        assertEquals(df.format(new BigDecimal(Double.toString(value))), "9.8258");// 不丢精度
        assertEquals(Utils.roundTo(value, -4), 9.8258, 0);

        // 默认格式化方式
        DecimalFormat df2 = new DecimalFormat();
        assertEquals(df2.format(value), "9.826");

        assertEquals(Utils.formatFloat("0.####", value), "9.8258");

    }

    @Test
    public void test_roundTo_2() {
        assertEquals("舍入测试", roundTo(1.234, -2), 1.23, 0);
        assertEquals("进一测试", roundTo(1.235, 0), 1.0, 0);
        assertEquals("进一测试", roundTo(1.245, 0), 1.0, 0);
        assertEquals("进一测试", roundTo(11.5, 0), 12.0, 0);
        assertEquals("银行家算法测试", roundTo(10.5, 0), 10.0, 0);
        assertEquals("银行家算法测试", roundTo(10.45, -1), 10.4, 0);
        assertEquals("银行家算法测试", roundTo(10.55, -1), 10.6, 0);
        assertEquals("负数测试", roundTo(-12.3, 0), -12.0, 0);
        assertEquals("负数测试", roundTo(-9.82501, -2), -9.83, 0);
    }

    @Test
    public void test_Trunc() {
        assertEquals(trunc(-123.55), -123.00, 0);
    }

    @Test
    public void test_ceil() {
        assertEquals(ceil(-123.55), -123, 0);
        assertEquals(ceil(123.15), 124, 0);
        assertEquals(ceil(123), 123, 0);
        assertEquals(ceil(-123.1), -123, 0);
    }

    @Test
    public void test_safeString() {
        String str = "' and '='1";
        assertEquals(Utils.safeString(str), "'' and ''=''1");
    }

    @Test
    public void test_assigned() {
        String obj = null;
        assertTrue(!assigned(obj));
        obj = "";
        assertTrue(assigned(obj));
    }

    @Test
    public void test_isNumeric() {
        assertTrue(isNumeric("111.333"));
        assertTrue(isNumeric("1113232333"));
        assertTrue(!isNumeric("a111.333"));
    }

    @Test
    public void test_copy() {
        assertEquals("a", copy("abcd", 1, 1));
        assertEquals("abcd", copy("abcd", 1, 5));
        assertEquals("bcd", copy("abcd", 2, 5));
        assertEquals("", copy(null, 2, 5));
    }

    @Test
    public void test_floatToStr() {
        assertEquals("1.3", floatToStr(1.30));
        assertEquals("0.0", floatToStr(0.00));
        assertEquals("-2.02", floatToStr(-2.02));
    }

    @Test
    public void test_strToFloatDef() {
        assertEquals(1.03, strToDoubleDef("1.03", 0.0), 0);
        assertEquals(0, strToDoubleDef("1.03a", 0), 0);
        assertEquals(-1.03, strToDoubleDef("-1.03", 0), 0);
    }
}
