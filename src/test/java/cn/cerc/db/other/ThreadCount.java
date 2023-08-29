package cn.cerc.db.other;

public class ThreadCount {
    private static Object obj = new Object();
    private static int count = 0;

    public static void main(String[] args) {
        for (;;) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    synchronized (obj) {
                        count += 1;
                        System.out.println("Thread #" + count);
                    }
                    for (;;) {
                        try {
                            Thread.sleep(1000);
                        } catch (Exception e) {
                            System.err.println(e);
                        }
                    }
                }
            }).start();
        }
    }
}