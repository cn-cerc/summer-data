package cn.cerc.db.log;

public class KnowallLogTest {

    public static void main(String[] args) {
        var log = new KnowallLog("KnowallLogTest");
        log.setMessage("测试");
        log.post();
    }

}
