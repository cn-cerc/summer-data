package cn.cerc.db.redis;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import cn.cerc.db.core.Handle;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.StubSession;
import cn.cerc.db.core.Utils;
import cn.cerc.db.mysql.MysqlQuery;
import cn.cerc.db.mysql.MysqlServerMaster;
import cn.cerc.db.mysql.Transaction;

public class LockerTest {

    private static final IHandle handle1 = new Handle(new StubSession());
    private static final IHandle handle2 = new Handle(new StubSession());
    private static final String Table = "TranNoDate";
    private static final String Tb = "LK";
    private static final String Date = "20231128";
    private static final String LockKey = String.join(".", Date, Tb);

    @Before
    public void setup() {
        MysqlServerMaster obj1 = new MysqlServerMaster();
        MysqlServerMaster obj2 = new MysqlServerMaster();
        handle1.getSession().setProperty(MysqlServerMaster.SessionId, obj1);
        handle2.getSession().setProperty(MysqlServerMaster.SessionId, obj2);
        handle1.getSession().setProperty(ISession.CORP_NO, "911001");
        handle2.getSession().setProperty(ISession.CORP_NO, "911001");
    }

    @Test
    public void test() throws InterruptedException, SQLException {
        demo_error_1();// 多线程环境，修改同一条数据，导致乐观锁问题
        demo_error_2();// 多线程环境，修改同一条数据，错误使用锁，导致乐观锁问题
        demo_correct_3();// 多线程环境，修改同一条数据，正确使用锁，解决乐观锁问题
    }

    /**
     * 模拟 业务A和业务B查出来的数据一致,“业务B”开启事务，“业务A”在“业务B”事务未提交之前，业务A执行修改，等待业务B事务提交，业务A执行修改失败
     * 
     * @throws SQLException
     */
    public void demo_error_1() throws InterruptedException, SQLException {
        // 业务A
        Thread thread = new Thread(() -> {
            try {
                System.err.println(String.format("业务A开始执行，事务自动提交:%s",
                        handle1.getMysql().getClient().getConnection().getAutoCommit()));
                // 模拟其他操作耗时
                Thread.sleep(500);
                updateData(handle1, "业务A");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                assertEquals(RuntimeException.class, e.getClass());
                e.printStackTrace();
            }
            // 业务A后增加流水号
            System.err.println("业务A执行完成");
        });

        // 业务B
        try (Transaction tx = new Transaction(handle2)) {
            System.err.println(
                    String.format("业务B开始执行，事务自动提交:%s", handle2.getMysql().getClient().getConnection().getAutoCommit()));
            // 业务B先增加流水号
            updateData(handle2, "业务B");
            thread.start();
            Thread.sleep(700);
            // 模拟其他操作耗时
            tx.commit();
            System.err.println("业务B执行完成");
        }
        thread.join();
    }

    /**
     * 模拟 “业务B”，还未提交事务，已经释放了锁，导致“业务A”进入执行，查到了久数据，因为“业务B”开启了事情，“业务A”无法提交
     */
    public void demo_error_2() throws InterruptedException {
        // 业务A
        Thread thread = new Thread(() -> {
            try (Locker locker = new Locker(LockerTest.class.getSimpleName(), LockKey)) {
                if (locker.lock("业务A", 3000)) {
                    System.err.println("业务A开始执行");
                    Thread.sleep(500);// 模拟其他操作耗时
                    updateData(handle1, "业务A");// 业务A后增加流水号
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                assertEquals(RuntimeException.class, e.getClass());
                throw e;
            }
            // 业务A后增加流水号
            System.err.println("业务A执行完成");
        });

        // 业务B
        try (Transaction tx = new Transaction(handle2)) {
            System.err.println("业务B开始执行");
            try (Locker locker = new Locker(LockerTest.class.getSimpleName(), LockKey)) {
                if (locker.lock("业务B", 3000))
                    updateData(handle2, "业务B");// 业务B先增加流水号
            }
            thread.start();// 启动A业务执行
            Thread.sleep(600);// 模拟其他操作耗时
            tx.commit();
            System.err.println("业务B执行完成");
        }
        thread.join();
    }

    /**
     * 模拟 “业务A”等待“业务B”事务提交之后才执行，此时查出来的数据是最新的，避免了乐观锁问题
     */
    public void demo_correct_3() throws InterruptedException {
        // 业务A
        Thread thread = new Thread(() -> {
            try (Locker locker = new Locker(LockerTest.class.getSimpleName(), LockKey)) {
                if (locker.lock("业务A", 3000)) {
                    System.err.println("业务A开始执行");
                    Thread.sleep(500);// 模拟其他操作耗时
                    updateData(handle1, "业务A");// 业务A后增加流水号
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                assertEquals(RuntimeException.class, e.getClass());
                throw e;
            }
            // 业务A后增加流水号
            System.err.println("业务A执行完成");
        });

        // 业务B
        try (Locker locker = new Locker(LockerTest.class.getSimpleName(), LockKey);
                Transaction tx = new Transaction(handle2)) {
            if (locker.lock("业务B", 3000)) {
                System.err.println("业务B开始执行");
                updateData(handle2, "业务B");// 业务B先增加流水号
                thread.start();// 启动A业务执行
                Thread.sleep(600);// 模拟其他操作耗时
                tx.commit();
                System.err.println("业务B执行完成");
            }
        }
        thread.join();
    }

    private void updateData(IHandle handle, String flag) {
        MysqlQuery query = new MysqlQuery(handle);
        query.add("select * from %s", Table);
        query.addWhere().eq("CorpNo_", handle.getCorpNo()).eq("TB_", Tb).eq("Date_", Date).build();
        query.setMaximum(1);
        query.open();
        System.out.println(flag + ":" + query.json());
        if (query.eof()) {
            query.append();
            query.setValue("UpdateKey_", Utils.newGuid());
            query.setValue("TB_", Tb);
            query.setValue("Date_", Date);
            query.setValue("LastNo_", 1);
            query.setValue("CorpNo_", handle.getCorpNo());
        } else {
            int lastNo = query.getInt("LastNo_");
            query.edit();
            query.setValue("LastNo_", lastNo + 1);
        }
        query.post();
    }

}
