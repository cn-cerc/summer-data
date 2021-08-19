package cn.cerc.core;

import junit.framework.TestCase;
import org.junit.Test;

public class DataSetTest extends TestCase {

    /**
     * 小数点测试
     */
    @Test
    public void test_1() {
        String json = "{\"head\":{\"Discount_\":0.91,\"MaxCouponAmount_\":-1},\"dataset\":[[\"PartCode_\",\"Desc_\",\"Spec_\",\"Unit_\"],[\"21CJ7H1009-1\",\"鲇竿,长江七号,10091\",\"专卖,碳素,40T,565g,1009,OFF.CC\",\"PCS\"],[\"21CJ7H1110-1\",\"鲇竿,长江七号,11101\",\"专卖,碳素,40T,665g,1110,OFF.CC\",\"PCS\"],[\"21CJ7H1210-1\",\"鲇竿,长江七号,1210\",\"专卖,碳素,40T,720g,1210,OFF.CC\",\"PCS\"],[\"21CJ7H9008-1\",\"鲇竿,长江七号,90081\",\"专卖,碳素,40T,450g,9008,OFF.CC\",\"PCS\"],[\"21ADF4505-1\",\"鼎风,4501\",\"专卖\\u0027碳素,40T,127g,4501\",\"PCS\"]]}";
        System.out.println(json);
        DataSet dataSet = new DataSet().fromJson(json);
        System.out.println(dataSet);
        System.out.println(dataSet.getHead().hasValue("Discount_"));
        System.out.println(dataSet.getHead().getString("Discount_"));
        System.out.println(dataSet.getHead().getInt("Discount_"));
        System.out.println("".equals(dataSet.getHead().getString("Discount_")));
    }

    /**
     * 整数测试
     */
    @Test
    public void test_2() {
        String json = "{\"head\":{\"Discount_\":1,\"MaxCouponAmount_\":-1},\"dataset\":[[\"PartCode_\",\"Desc_\",\"Spec_\",\"Unit_\"],[\"21CJ7H1009-1\",\"鲇竿,长江七号,10091\",\"专卖,碳素,40T,565g,1009,OFF.CC\",\"PCS\"],[\"21CJ7H1110-1\",\"鲇竿,长江七号,11101\",\"专卖,碳素,40T,665g,1110,OFF.CC\",\"PCS\"],[\"21CJ7H1210-1\",\"鲇竿,长江七号,1210\",\"专卖,碳素,40T,720g,1210,OFF.CC\",\"PCS\"],[\"21CJ7H9008-1\",\"鲇竿,长江七号,90081\",\"专卖,碳素,40T,450g,9008,OFF.CC\",\"PCS\"],[\"21ADF4505-1\",\"鼎风,4501\",\"专卖\\u0027碳素,40T,127g,4501\",\"PCS\"]]}";
        System.out.println(json);
        DataSet dataSet = new DataSet().fromJson(json);
        System.out.println(dataSet);
        System.out.println(dataSet.getHead().hasValue("Discount_"));
        System.out.println(dataSet.getHead().getString("Discount_"));
        System.out.println(dataSet.getHead().getInt("Discount_"));
        System.out.println("".equals(dataSet.getHead().getString("Discount_")));
    }
}