package cn.cerc.db.testsql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import cn.cerc.db.core.DataSet;

public class SqlWhereFilterTest {

    @Test
    public void test_eq() {
        var obj = new SqlWhereFilter("where a=1 and b=2");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1);
        ds.append().setValue("a", 1).setValue("b", 2);
        ds.append().setValue("a", 2).setValue("b", 3);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b"],[1,2]]}""", ds.toString());
    }

    @Test
    public void test_eq_2() {
        var obj = new SqlWhereFilter("where a=1");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1);
        ds.append().setValue("a", 1).setValue("b", 2);
        ds.append().setValue("a", 2).setValue("b", 3);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b"],[1,1],[1,2]]}""", ds.toString());
    }

    @Test
    public void test_gt() {
        var obj = new SqlWhereFilter("where a > 2");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1);
        ds.append().setValue("a", 1).setValue("b", 2);
        ds.append().setValue("a", 5).setValue("b", 3);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b"],[5,3]]}""", ds.toString());
    }

    @Test
    public void test_gte() {
        var obj = new SqlWhereFilter("where a >= 2");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1);
        ds.append().setValue("a", 2).setValue("b", 2);
        ds.append().setValue("a", 5).setValue("b", 3);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b"],[2,2],[5,3]]}""", ds.toString());
    }

    @Test
    public void test_lt() {
        var obj = new SqlWhereFilter("where a < 2");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1);
        ds.append().setValue("a", 2).setValue("b", 2);
        ds.append().setValue("a", 5).setValue("b", 3);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b"],[1,1]]}""", ds.toString());
    }

    @Test
    public void test_lte() {
        var obj = new SqlWhereFilter("where a <= 2");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1);
        ds.append().setValue("a", 2).setValue("b", 2);
        ds.append().setValue("a", 5).setValue("b", 3);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b"],[1,1],[2,2]]}""", ds.toString());
    }

    @Test
    public void test_ne() {
        var obj = new SqlWhereFilter("where a != 2 and b <> 1");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1);
        ds.append().setValue("a", 2).setValue("b", 2);
        ds.append().setValue("a", 5).setValue("b", 3);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b"],[5,3]]}""", ds.toString());
    }

    @Test
    public void test_str_eq() {
        var obj = new SqlWhereFilter("where name = 'Jack'");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack").setValue("address", "A");
        ds.append().setValue("name", "Jackson").setValue("address", "B");
        ds.append().setValue("name", "Jenny").setValue("address", "C");
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["name","address"],["Jack","A"]]}""", ds.toString());
    }

    @Test
    public void test_str_ne() {
        var obj = new SqlWhereFilter("where name != 'Jack'");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack").setValue("address", "A");
        ds.append().setValue("name", "Jackson").setValue("address", "B");
        ds.append().setValue("name", "Jenny").setValue("address", "C");
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["name","address"],["Jackson","B"],["Jenny","C"]]}""", ds.toString());
    }

    @Test
    public void test_str_like() {
        var obj = new SqlWhereFilter("where name like 'Ja%' ");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack").setValue("address", "AA");
        ds.append().setValue("name", "Jackson").setValue("address", "AB");
        ds.append().setValue("name", "AJa").setValue("address", "CB");
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["name","address"],["Jack","AA"],["Jackson","AB"]]}""", ds.toString());
    }

    @Test
    public void test_time_gt() {
        var obj = new SqlWhereFilter("where order_time_ > '2023-08-01 15:08:41' ");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack").setValue("order_time_", "2023-08-01 15:08:40");
        ds.append().setValue("name", "Jackson").setValue("order_time_", "2023-08-01 15:08:41");
        ds.append().setValue("name", "Jenny").setValue("order_time_", "2023-08-01 15:08:42");
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["name","order_time_"],["Jenny","2023-08-01 15:08:42"]]}""", ds.toString());
    }

    @Test
    public void test_time_gte() {
        var obj = new SqlWhereFilter("where order_time_ >= '2023-08-01 15:08:41' ");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack").setValue("order_time_", "2023-08-01 15:08:40");
        ds.append().setValue("name", "Jackson").setValue("order_time_", "2023-08-01 15:08:41");
        ds.append().setValue("name", "Jenny").setValue("order_time_", "2023-08-01 15:08:42");
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["name","order_time_"],["Jackson","2023-08-01 15:08:41"],["Jenny","2023-08-01 15:08:42"]]}""",
                ds.toString());
    }

    @Test
    public void test_time_lt() {
        var obj = new SqlWhereFilter("where order_time_ < '2023-08-01 15:08:41' ");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack").setValue("order_time_", "2023-08-01 15:08:40");
        ds.append().setValue("name", "Jackson").setValue("order_time_", "2023-08-01 15:08:41");
        ds.append().setValue("name", "Jenny").setValue("order_time_", "2023-08-01 15:08:42");
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["name","order_time_"],["Jack","2023-08-01 15:08:40"]]}""", ds.toString());
    }

    @Test
    public void test_time_lte() {
        var obj = new SqlWhereFilter("where order_time_ <= '2023-08-01 15:08:41' ");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack").setValue("order_time_", "2023-08-01 15:08:40");
        ds.append().setValue("name", "Jackson").setValue("order_time_", "2023-08-01 15:08:41");
        ds.append().setValue("name", "Jenny").setValue("order_time_", "2023-08-01 15:08:42");
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["name","order_time_"],["Jack","2023-08-01 15:08:40"],["Jackson","2023-08-01 15:08:41"]]}""",
                ds.toString());
    }

    @Test
    public void test_is_null() {
        var obj = new SqlWhereFilter("where order_time_ is null");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack");
        ds.append().setValue("name", "Jackson").setValue("order_time_", "2023-08-01 15:08:41");
        ds.append().setValue("name", "Jenny").setValue("order_time_", "2023-08-01 15:08:42");
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["name","order_time_"],["Jack",null]]}""", ds.toString());
    }

    @Test
    public void test_is_not_null() {
        var obj = new SqlWhereFilter("where order_time_ is not null");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack");
        ds.append().setValue("name", "Jackson").setValue("order_time_", "2023-08-01 15:08:41");
        ds.append().setValue("name", "Jenny").setValue("order_time_", "2023-08-01 15:08:42");
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["name","order_time_"],["Jackson","2023-08-01 15:08:41"],["Jenny","2023-08-01 15:08:42"]]}""",
                ds.toString());
    }

    @Test
    public void test_in_num() {
        var obj = new SqlWhereFilter("where a in (1,5)");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1);
        ds.append().setValue("a", 2).setValue("b", 2);
        ds.append().setValue("a", 5).setValue("b", 3);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b"],[1,1],[5,3]]}""", ds.toString());
    }

    @Test
    public void test_in_str() {
        var obj = new SqlWhereFilter("where name in ('Jack', 'Jenny')");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack");
        ds.append().setValue("name", "Jackson").setValue("order_time_", "2023-08-01 15:08:41");
        ds.append().setValue("name", "Jenny").setValue("order_time_", "2023-08-01 15:08:42");
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["name","order_time_"],["Jack",null],["Jenny","2023-08-01 15:08:42"]]}""", ds.toString());
    }

    @Test
    public void test_or1() {
        var obj = new SqlWhereFilter("where name='Jack' or name='Jenny' and age=18");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack");
        ds.append().setValue("name", "Jenny").setValue("age", 18);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals(2, ds.size());
    }

    @Test
    public void test_or2() {
        var obj = new SqlWhereFilter("where name='Jack' or name='Jenny' and age=18 or sex='女'");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack");
        ds.append().setValue("name", "Jenny").setValue("age", 18);
        ds.append().setValue("sex", "女");
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals(3, ds.size());
    }

    @Test
    public void test_or3() {
        var obj = new SqlWhereFilter("where name='Jack' or name='Jenny' and age=18 or sex='女' and name='王'");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack");
        ds.append().setValue("name", "Jenny").setValue("age", 18);
        ds.append().setValue("sex", "女");
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals(2, ds.size());
    }

}
