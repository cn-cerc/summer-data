package cn.cerc.db.redis;

import cn.cerc.db.core.LanguageResource;

public class LockerTest {

    // a: 正常执时
    private static Runnable job1_a = () -> {
        try (Locker locker = new Locker(LockerTest.class.getSimpleName(), "test")) {
            if (locker.requestLock("job1", 3000))
                sleep(5000);
        }
    };
    // b: 超时锁定
    private static Runnable job1_b = () -> {
        try (Locker locker = new Locker(LockerTest.class.getSimpleName(), "test")) {
            if (locker.requestLock("job1", 3000)) {
                sleep(15000);
            }
        }
    };
    // c: 超长执行
    private static Runnable job1_c = () -> {
        try (Locker locker = new Locker(LockerTest.class.getSimpleName(), "test")) {
            if (locker.requestLock("job1", 3000)) {
                // c:测试 renewal 函数的使用
                sleep(5000);
                locker.renewal();
                sleep(5000);
                locker.renewal();
                sleep(5000);
            }
        }
    };
    
    private static Runnable job2 = () -> {
        try (Locker locker = new Locker(LockerTest.class.getSimpleName(), "test")) {
            locker.requestLock("job2", 10000);
        }
    };

    private static void sleep(int timer) {
        try {
            Thread.sleep(timer);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        // 将资源文件输出设置为中文
        LanguageResource.appLanguage = "cn";

        System.out.println("************* 测试: 排队执行 *************");
        new Thread(job1_a).start();
        sleep(1000); // 等待执行完成
        new Thread(job2).start();
        sleep(20 * 1000); // 等待执行完成
        
        System.out.println("************* 测试：强制锁定 *************");
        new Thread(job1_b).start();
        sleep(1000); // 等待执行完成
        new Thread(job2).start();
        sleep(20 * 1000); // 等待执行完成

        System.out.println("************* 测试：超长耗时 *************");
        new Thread(job1_c).start();
        sleep(1000); // 等待执行完成
        new Thread(job2).start();
        sleep(20 * 1000); // 等待执行完成
        
    }

}
