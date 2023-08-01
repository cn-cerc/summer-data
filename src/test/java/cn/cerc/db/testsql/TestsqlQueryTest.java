package cn.cerc.db.testsql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestsqlQueryTest {

    @Test
    public void test_select() {
        var db = TestsqlServer.get();
        db.onSelect("sss", (query, sql) -> {
            query.append();
            query.setValue("UID_", 1);
            query.setValue("code_", "001");
            query.append();
            query.setValue("UID_", 2);
            query.setValue("code_", "002");
            query.append();
            query.setValue("UID_", 3);
            query.setValue("code_", "003");
        });
        TestsqlQuery query = new TestsqlQuery();
        query.add("select * from sss");
        query.open();
        assertEquals("""
                {"body":[["UID_","code_"],[1,"001"],[2,"002"],[3,"003"]]}""", query.toString());
    }

    @Test
    public void test_delete() {
        var db = TestsqlServer.get();
        db.onSelect("sss", (query, sql) -> {
            query.setJson("""
                    {"body":[["UID_","code_"],[1,"001"],[2,"002"],[3,"003"]]}""");
        });
        TestsqlQuery query = new TestsqlQuery();
        query.add("select * from sss");
        query.open();

        query.first();
        query.delete();
        assertEquals("""
                {"body":[["UID_","code_"],[2,"002"],[3,"003"]]}""", query.toString());
    }

    @Test
    public void test_insert() {
        var db = TestsqlServer.get();
        db.onSelect("sss", (query, sql) -> {
            query.setJson("""
                    {"body":[["UID_","code_"],[1,"001"],[2,"002"],[3,"003"]]}""");
        });
        TestsqlQuery query = new TestsqlQuery();
        query.add("select * from sss");
        query.open();

        query.append();
        query.setValue("code_", "005");
        query.post();
        assertEquals("""
                {"body":[["UID_","code_"],[1,"001"],[2,"002"],[3,"003"],[4,"005"]]}""", query.toString());
    }

    @Test
    public void test_update() {
        var db = TestsqlServer.get();
        db.onSelect("sss", (query, sql) -> {
            query.setJson("""
                    {"body":[["UID_","code_"],[1,"001"],[2,"002"],[3,"003"]]}""");
        });
        TestsqlQuery query = new TestsqlQuery();
        query.add("select * from sss");
        query.open();

        query.edit();
        query.setValue("code_", "005");
        query.post();
        assertEquals("""
                {"body":[["UID_","code_"],[1,"005"],[2,"002"],[3,"003"]]}""", query.toString());
    }

}
