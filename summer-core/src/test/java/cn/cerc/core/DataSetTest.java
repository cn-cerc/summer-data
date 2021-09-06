package cn.cerc.core;

import org.junit.Test;

import junit.framework.TestCase;

public class DataSetTest extends TestCase {
    
    @Test
    public void test_list() {
        DataSet ds = new DataSet();
        ds.append();
        ds.setField("A", "A01");
        ds.setField("B", "B01");
        ds.append();
        ds.setField("A", "A02");
        ds.setField("B", "B02");
        ds.append();
        ds.setField("A", "A03");
        ds.setField("B", "B03");

        int i = 0;
        for (@SuppressWarnings("unused")
        Record record : ds) {
            i++;
        }
        assertEquals(i, ds.size());
    }

    @Test
    public void test_delete_a() {
        DataSet ds = new DataSet();
        ds.append().setField("code", "a");
        ds.append().setField("code", "b");
        ds.append().setField("code", "c");

        int i = 0;
        ds.first();
        while (!ds.eof()) {
            i++;
            if (ds.getString("code").equals("a"))
                ds.delete();
            else
                ds.next();
        }
        assertEquals(i, 3);
    }

    @Test
    public void test_delete_b() {
        DataSet ds = new DataSet();
        ds.append().setField("code", "a");
        ds.append().setField("code", "b");
        ds.append().setField("code", "c");

        int i = 0;
        ds.first();
        while (!ds.eof()) {
            i++;
            if (ds.getString("code").equals("b"))
                ds.delete();
            else
                ds.next();
        }
        assertEquals(i, 3);
    }

    @Test
    public void test_delete_c() {
        DataSet ds = new DataSet();
        ds.append().setField("code", "a");
        ds.append().setField("code", "b");
        ds.append().setField("code", "c");

        int i = 0;
        ds.first();
        while (!ds.eof()) {
            i++;
            if (ds.getString("code").equals("c"))
                ds.delete();
            else
                ds.next();
        }
        assertEquals(i, 3);
    }

    @Test
    public void test_toJSON() {
        DataSet ds = new DataSet();
        String jsonStr = "{\"head\":{\"It\":1,\"TBNo\":\"OD001\"},"
                + "\"body\":[[\"It\",\"Part\",\"Desc\"],[1,\"001\",\"desc\"],[2,\"001\",\"desc\"]]}";
        Record head = ds.getHead();
        head.setField("It", 1);
        head.setField("TBNo", "OD001");
        ds.append();
        ds.setField("It", 1);
        ds.setField("Part", "001");
        ds.setField("Desc", "desc");
        ds.append();
        ds.setField("It", 2);
        ds.setField("Part", "001");
        ds.setField("Desc", "desc");
        assertEquals(jsonStr, ds.toString());
    }

    @Test
    public void test_fromJSON() {
        String jsonStr = "{\"head\":{\"It\":1,\"TBNo\":\"OD001\"},"
                + "\"body\":[[\"It\",\"Part\",\"Desc\"],[1,\"001\",\"desc\"],[2,\"001\",\"desc\"]]}";
        DataSet ds = new DataSet().fromJson(jsonStr);
        assertEquals(jsonStr, ds.toString());
        assertEquals("1", ds.getHead().getString("It"));
    }

    /**
     * 小数点测试
     */
    @Test
    public void test_1() {
        String json = "{\"head\":{\"Discount_\":0.91,\"MaxCouponAmount_\":-1},\"body\":[[\"PartCode_\",\"Desc_\",\"Spec_\",\"Unit_\"],[\"21CJ7H1009-1\",\"鲇竿,长江七号,10091\",\"专卖,碳素,40T,565g,1009,OFF.CC\",\"PCS\"],[\"21CJ7H1110-1\",\"鲇竿,长江七号,11101\",\"专卖,碳素,40T,665g,1110,OFF.CC\",\"PCS\"],[\"21CJ7H1210-1\",\"鲇竿,长江七号,1210\",\"专卖,碳素,40T,720g,1210,OFF.CC\",\"PCS\"],[\"21CJ7H9008-1\",\"鲇竿,长江七号,90081\",\"专卖,碳素,40T,450g,9008,OFF.CC\",\"PCS\"],[\"21ADF4505-1\",\"鼎风,4501\",\"专卖\\u0027碳素,40T,127g,4501\",\"PCS\"]]}";
        DataSet dataSet = new DataSet().fromJson(json);
        assertEquals(json, dataSet.toString());
        assertTrue(dataSet.getHead().hasValue("Discount_"));
        assertEquals("0.91", dataSet.getHead().getString("Discount_"));
        assertEquals(0, dataSet.getHead().getInt("Discount_"));
    }

    /**
     * 整数测试
     */
    @Test
    public void test_2() {
        String json = "{\"head\":{\"Discount_\":1,\"MaxCouponAmount_\":-1},\"dataset\":[[\"PartCode_\",\"Desc_\",\"Spec_\",\"Unit_\"],[\"21CJ7H1009-1\",\"鲇竿,长江七号,10091\",\"专卖,碳素,40T,565g,1009,OFF.CC\",\"PCS\"],[\"21CJ7H1110-1\",\"鲇竿,长江七号,11101\",\"专卖,碳素,40T,665g,1110,OFF.CC\",\"PCS\"],[\"21CJ7H1210-1\",\"鲇竿,长江七号,1210\",\"专卖,碳素,40T,720g,1210,OFF.CC\",\"PCS\"],[\"21CJ7H9008-1\",\"鲇竿,长江七号,90081\",\"专卖,碳素,40T,450g,9008,OFF.CC\",\"PCS\"],[\"21ADF4505-1\",\"鼎风,4501\",\"专卖\\u0027碳素,40T,127g,4501\",\"PCS\"]]}";
        DataSet dataSet = new DataSet().fromJson(json);
        assertTrue(dataSet.getHead().hasValue("Discount_"));
        assertEquals("1", dataSet.getHead().getString("Discount_"));
        assertEquals(1, dataSet.getHead().getInt("Discount_"));
    }
}