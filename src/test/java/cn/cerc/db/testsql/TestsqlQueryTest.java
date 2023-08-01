package cn.cerc.db.testsql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import cn.cerc.db.mysql.MysqlQuery;

public class TestsqlQueryTest {

    @Test
    public void test_select() {
        var db = TestsqlServer.build();
        db.onSelect("sss", (query, sql) -> {
            query.setJson("""
                    {"body":[["UID_","code_"],[1,"001"],[2,"002"],[3,"003"]]}""");
        });
        TestsqlQuery query = new TestsqlQuery();
        query.add("select * from sss");
        query.open();
        assertEquals("""
                {"body":[["UID_","code_"],[1,"001"],[2,"002"],[3,"003"]]}""", query.toString());
    }

    @Test
    public void test_delete() {
        var db = TestsqlServer.build();
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
        var db = TestsqlServer.build();
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
        var db = TestsqlServer.build();
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

    @Test
    public void test_mysql() {
        var db = TestsqlServer.build();
        db.onSelect("sss", (query, sql) -> {
            query.setJson("""
                    {"body":[["UID_","code_"],[1,"001"],[2,"002"],[3,"003"]]}""");
        });
        MysqlQuery query = new MysqlQuery();
        query.add("select * from sss");
        query.open();
        assertEquals("""
                {"body":[["UID_","code_"],[1,"001"],[2,"002"],[3,"003"]]}""", query.toString());
    }

}
