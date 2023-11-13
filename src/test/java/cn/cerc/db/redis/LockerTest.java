package cn.cerc.db.redis;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class LockerTest {

    private final String group = LockerTest.class.getSimpleName() + ".group";
    private final String firstKey = LockerTest.class.getSimpleName() + ".key";
    private final int count = 5000;

    @Test
    public void lock_test1() {
        try (Locker lock = new Locker(group, firstKey)) {
            assertEquals(lock.lock("标记", 1000), true);
            assertEquals(lock.lock("标记", 1000), false);
        }
    }

    @Test
    public void lock_test2() {
        try (Locker lock = new Locker(group, firstKey)) {
            assertEquals(lock.lock("标记", 1000), true);
        }
        try (Locker lock = new Locker(group, firstKey)) {
            assertEquals(lock.lock("标记", 1000), true);
        }
    }

    @Test
    public void lock_test3() {
        try (Locker lock = new Locker(group, firstKey)) {
            assertEquals(lock.lock("标记", 1000), true);
        }
        try (Locker lock = new Locker(group, firstKey)) {
            assertEquals(lock.lock("标记", 1000), true);
        }
    }

    @Test
    public void lockTransferMoney() throws InterruptedException {
        BankAccount account1 = new BankAccount(1000);
        BankAccount account2 = new BankAccount(1000);

        Thread t1 = new Thread(() -> {
            for (int i = 0; i < count; i++) {
                try (Locker lock = new Locker(group, firstKey)) {
                    if (lock.lock("标记", 1000))
                        account1.transfer(account2, 10);
                    else
                        i--;
                }
            }
        });
        Thread t2 = new Thread(() -> {
            for (int i = 0; i < count; i++) {
                try (Locker lock = new Locker(group, firstKey)) {
                    if (lock.lock("标记", 1000))
                        account2.transfer(account1, 10);
                    else
                        i--;
                }
            }
        });
        Thread t3 = new Thread(() -> {
            for (int i = 0; i < count; i++) {
                try (Locker lock = new Locker(group, firstKey)) {
                    if (lock.lock("标记", 1000))
                        account1.transfer(account2, 10);
                    else
                        i--;
                }
            }
        });
        Thread t4 = new Thread(() -> {
            for (int i = 0; i < count; i++) {
                try (Locker lock = new Locker(group, firstKey)) {
                    if (lock.lock("标记", 1000))
                        account2.transfer(account1, 10);
                    else
                        i--;
                }
            }
        });

        t1.start();
        t2.start();
        t3.start();
        t4.start();

        try {
            t1.join();
            t2.join();
            t3.join();
            t4.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.err.println("lockTransferMoney Final balance of account1: " + account1.getBalance());
        System.err.println("lockTransferMoney Final balance of account2: " + account2.getBalance());
        assertEquals(account1.getBalance() == account2.getBalance(), true);
    }

    @Test
    public void notLockTransferMoney() throws InterruptedException {
        BankAccount account1 = new BankAccount(1000);
        BankAccount account2 = new BankAccount(1000);

        Thread t1 = new Thread(() -> {
            for (int i = 0; i < count; i++)
                account1.transfer(account2, 10);
        });
        Thread t2 = new Thread(() -> {
            for (int i = 0; i < count; i++)
                account2.transfer(account1, 10);
        });
        Thread t3 = new Thread(() -> {
            for (int i = 0; i < count; i++)
                account1.transfer(account2, 10);
        });
        Thread t4 = new Thread(() -> {
            for (int i = 0; i < count; i++)
                account2.transfer(account1, 10);
        });

        t1.start();
        t2.start();
        t3.start();
        t4.start();

        try {
            t1.join();
            t2.join();
            t3.join();
            t4.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.err.println("notLockTransferMoney Final balance of account1: " + account1.getBalance());
        System.err.println("notLockTransferMoney Final balance of account2: " + account2.getBalance());
        assertEquals(account1.getBalance() == account2.getBalance(), false);
    }

    class BankAccount {
        private int balance;

        public BankAccount(int balance) {
            this.balance = balance;
        }

        public int getBalance() {
            return balance;
        }

        public void transfer(BankAccount target, int amount) {
            this.balance -= amount;
            target.balance += amount;
        }
    }

}
