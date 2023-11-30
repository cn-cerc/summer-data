package cn.cerc.db.redis;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import cn.cerc.db.core.Handle;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.StubSession;
import cn.cerc.db.core.Utils;
import cn.cerc.db.mysql.MysqlQuery;
import cn.cerc.db.mysql.MysqlServerMaster;
import cn.cerc.db.mysql.Transaction;

public class LockerTest2 {

    private static final IHandle Handle = new Handle(new StubSession());
    private static final String Table = "TranNoDate";
    private static final String Tb = "LK";
    private static final String Date = "20231128";

    @Before
    public void setup() {
        MysqlServerMaster obj = new MysqlServerMaster();
        Handle.getSession().setProperty(MysqlServerMaster.SessionId, obj);
    }

    @Test
    public void test() throws InterruptedException, SQLException {
        updateDataA();
    }

    @Test
    public void test1() throws InterruptedException, SQLException {
        svrTranNoDateBase("222002");
    }

    public boolean updateDataA() {
        String corpNo1 = "227001";
        String corpNo2 = "222002";
        String lockKey1 = String.join(".", corpNo1, Tb, Date);
        String lockKey2 = String.join(".", corpNo2, Tb, Date);
        try (Locker lock = new Locker(LockerTest2.class.getSimpleName(), lockKey1, lockKey2);
                Transaction tx = new Transaction(Handle)) {
            String flag = String.join(".", LockerTest2.class.getSimpleName(), "updateDataA");
            if (!lock.lock(flag, 3000))
                throw new RuntimeException(String.format("%s is locked", flag));
            svrTranNoDateBase(corpNo1);
            svrTranNoDateBase(corpNo2);
            tx.commit();
        }
        return true;
    }

    public boolean svrTranNoDateBase(String corpNo) {
        String lockKey = String.join(".", corpNo, Tb, Date);
        try (Locker lock = new Locker(LockerTest2.class.getSimpleName(), lockKey)) {
            String flag = String.join(".", LockerTest2.class.getSimpleName(), "svrTranNoDateBase");
            if (!lock.lock(lockKey, 3000))
                throw new RuntimeException(String.format("%s is locked", flag));
            MysqlQuery query = new MysqlQuery(Handle);
            query.add("select * from %s", Table);
            query.addWhere().eq("CorpNo_", corpNo).eq("TB_", Tb).eq("Date_", Date).build();
            query.setMaximum(1);
            query.open();
            System.out.println(flag + ":" + query.json());
            if (query.eof()) {
                query.append();
                query.setValue("UpdateKey_", Utils.newGuid());
                query.setValue("TB_", Tb);
                query.setValue("Date_", Date);
                query.setValue("LastNo_", 1);
                query.setValue("CorpNo_", corpNo);
            } else {
                int lastNo = query.getInt("LastNo_");
                query.edit();
                query.setValue("LastNo_", lastNo + 1);
            }
            query.post();
        }
        return true;
    }

}
