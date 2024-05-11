package cn.cerc.db.log;

public class KnowallLogTest {

    public static void main(String[] args) throws InterruptedException {
        var log = new KnowallLog(KnowallLogTest.class, 6);
        log.setMessage("测试");
        log.post(response -> System.out.println(response));
    }

}
