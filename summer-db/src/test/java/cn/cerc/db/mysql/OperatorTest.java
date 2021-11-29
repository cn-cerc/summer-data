package cn.cerc.db.mysql;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import cn.cerc.core.FieldMeta.FieldKind;
import cn.cerc.core.ISession;
import cn.cerc.core.DataRow;
import cn.cerc.core.SqlText;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.StubSession;

public class OperatorTest implements IHandle {
    private int maxTest = 50;

    private ISession session;

    @Before
    public void setUp() {
        session = new StubSession();
    }

    @Test
    @Ignore
    public void test_2_insert_new() {
        MysqlServerMaster conn = this.getMysql();
        conn.execute("delete from temp where name_='new'");
        MysqlQuery ds = new MysqlQuery(this);
        ds.getSqlText().setMaximum(0);
        ds.add("select * from temp");
        ds.open();
        for (int i = 0; i < maxTest; i++) {
            ds.append();
            ds.setValue("Code_", "new" + i);
            ds.setValue("Name_", "new");
            ds.setValue("Value_", i + 1);
            ds.post();
        }
    }

    @Test
    @Ignore
    public void test_3_insert_new() {
        MysqlOperator obj = new MysqlOperator(this);
        obj.setTableName("temp");
        for (int i = 0; i < maxTest; i++) {
            DataRow record = new DataRow();
            record.fields().add("UID_", FieldKind.Storage);
            record.fields().add("Code_", FieldKind.Storage);
            record.setValue("Code_", "code1");
            record.fields().add("Name_", FieldKind.Storage);
            record.setValue("Name_", "new");
            record.fields().add("Value_", FieldKind.Storage);
            record.setValue("Value_", i + 1);
            obj.insert(this.getMysql().getClient().getConnection(), record);
        }
    }

    @Test
    @Ignore
    public void test_4_update_new() {
        MysqlQuery ds = new MysqlQuery(this);
        ds.add("select * from temp");
        ds.open();
        while (ds.fetch()) {
            ds.edit();
            ds.setValue("Code_", ds.getString("Code_") + "a");
            ds.setValue("Value_", ds.getDouble("Value_") + 1);
            ds.post();
        }
    }

    @Test
    @Ignore
    public void test_6_delete_new() {
        MysqlQuery ds = new MysqlQuery(this);
        ds.add("select * from temp where Name_='new'");
        ds.open();
        while (!ds.eof())
            ds.delete();
    }

    @Test
    @Ignore
    public void test_findTableName() {
        String sql = "select * from Dept";
        assertEquals(SqlText.findTableName(sql), "Dept");
        sql = "select * from \r\n Dept";
        assertEquals(SqlText.findTableName(sql), "Dept");
        sql = "select * from \r\nDept";
        assertEquals(SqlText.findTableName(sql), "Dept");
        sql = "select * from\r\n Dept";
        assertEquals(SqlText.findTableName(sql), "Dept");
        sql = "select * FROM Dept";
        assertEquals(SqlText.findTableName(sql), "Dept");
    }

    @Override
    public ISession getSession() {
        return session;
    }

    @Override
    public void setSession(ISession session) {
        this.session = session;
    }
}
