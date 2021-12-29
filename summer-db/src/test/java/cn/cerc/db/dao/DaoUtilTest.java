package cn.cerc.db.dao;

import org.junit.Test;

import com.google.gson.Gson;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.Utils;

public class DaoUtilTest {

    @Test
    public void testCopy1() {
        DataRow record = new DataRow();
        record.setValue("ID_", Utils.newGuid());
        record.setValue("Code_", "18100101");
        record.setValue("Name_", "王五");
        record.setValue("Mobile_", "1350019101");
        UserTest user = record.asEntity(UserTest.class);
        System.out.println(new Gson().toJson(user));

        record = new DataRow();
        record.setValue("ID_", Utils.newGuid());
        record.setValue("Code_", "18100101");
        record.setValue("Name_", "王五");
        record.setValue("Mobile_", "1350019101");
        record.setValue("Web_", true);
        user = record.asEntity(UserTest.class);
        System.out.println(new Gson().toJson(user));
    }

    @Test(expected = RuntimeException.class)
    public void testCopy2() {
        DataRow record = new DataRow();
        record.setValue("ID_", Utils.newGuid());
        record.setValue("Code_", "18100101");
        record.setValue("Name_", "王五");
        UserTest user = new UserTest();
        record.saveToEntity(user);
    }

}
