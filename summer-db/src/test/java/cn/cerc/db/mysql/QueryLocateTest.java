package cn.cerc.db.mysql;

import cn.cerc.core.DataSet;

public class QueryLocateTest {
    private static void locateTest(int flag) {
        DataSet ds = new DataSet();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 50000; i++)
            ds.append().setValue("code", "a").setValue("value", i + 1);
        long end = System.currentTimeMillis() - start;
        start = System.currentTimeMillis();
        for (int i = 0; i < 50000; i++) {
            if (!ds.locate("code;value", "a", i + 1))
                System.err.println("error: " + i);
        }
        System.out.println(
                String.format("%d, size: %d, append: %sms, locate：%sms", flag, ds.size(), end, System.currentTimeMillis() - start));
    }

    public static void main(String[] args) {
        for (int i = 1; i < 11; i++)
            locateTest(i);
    }
}
