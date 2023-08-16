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
    public void test_not_in_num() {
        var obj = new SqlWhereFilter("where a not in (1,5)");
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
                {"body":[["a","b"],[2,2]]}""", ds.toString());
    }

    @Test
    public void test_not_in_str() {
        var obj = new SqlWhereFilter("where name not in ('Jack', 'Jenny')");
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
                {"body":[["name","order_time_"],["Jackson","2023-08-01 15:08:41"]]}""", ds.toString());
    }

    @Test
    public void test_or_num() {
        var obj = new SqlWhereFilter("where a = 1 or b = 3");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1);
        ds.append().setValue("a", 2).setValue("b", 2);
        ds.append().setValue("a", 3).setValue("b", 3);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b"],[1,1],[3,3]]}""", ds.toString());
    }

    @Test
    public void test_or_str() {
        var obj = new SqlWhereFilter("where name = 'Jackson' or order_time_ is null");
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
                {"body":[["name","order_time_"],["Jack",null],["Jackson","2023-08-01 15:08:41"]]}""", ds.toString());
    }

    @Test
    public void test_and_or_num() {
        var obj = new SqlWhereFilter("where a = 1 and c = 1 or b = 3");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1).setValue("c", 1);
        ds.append().setValue("a", 2).setValue("b", 2).setValue("c", 2);
        ds.append().setValue("a", 3).setValue("b", 3).setValue("c", 3);
        ds.append().setValue("a", 4).setValue("b", 4).setValue("c", 4);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b","c"],[1,1,1],[3,3,3]]}""", ds.toString());
    }

    @Test
    public void test_and_or_str() {
        var obj = new SqlWhereFilter("where name = 'Jackson' and age = 24 or order_time_ > '2023-08-01 15:08:41'");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack").setValue("age", 23);
        ds.append().setValue("name", "Jackson").setValue("order_time_", "2023-08-01 15:08:41").setValue("age", 24);
        ds.append().setValue("name", "Jenny").setValue("order_time_", "2023-08-01 15:08:42").setValue("age", 25);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals(
                """
                        {"body":[["name","age","order_time_"],["Jackson",24,"2023-08-01 15:08:41"],["Jenny",25,"2023-08-01 15:08:42"]]}""",
                ds.toString());
    }

    @Test
    public void test_or_and_num() {
        var obj = new SqlWhereFilter("where a = 2 or c = 1 and b = 3");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1).setValue("c", 1);
        ds.append().setValue("a", 2).setValue("b", 2).setValue("c", 2);
        ds.append().setValue("a", 3).setValue("b", 3).setValue("c", 3);
        ds.append().setValue("a", 4).setValue("b", 4).setValue("c", 4);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b","c"],[1,1,1]]}""", ds.toString());
    }

    @Test
    public void test_or_and_str() {
        var obj = new SqlWhereFilter("where name = 'Jenny' or age = 24 and order_time_ != '2023-08-01 15:08:42'");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack").setValue("age", 23);
        ds.append().setValue("name", "Jackson").setValue("order_time_", "2023-08-01 15:08:41").setValue("age", 24);
        ds.append().setValue("name", "Jenny").setValue("order_time_", "2023-08-01 15:08:42").setValue("age", 25);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["name","age","order_time_"],["Jackson",24,"2023-08-01 15:08:41"]]}""", ds.toString());
    }

    @Test
    public void test_bracket_1() {
        var obj = new SqlWhereFilter("where (a = 2 or c = 1) and b = 2");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1).setValue("c", 1);
        ds.append().setValue("a", 2).setValue("b", 2).setValue("c", 2);
        ds.append().setValue("a", 3).setValue("b", 3).setValue("c", 3);
        ds.append().setValue("a", 4).setValue("b", 4).setValue("c", 4);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b","c"],[2,2,2]]}""", ds.toString());
    }

    @Test
    public void test_bracket_2() {
        var obj = new SqlWhereFilter("where (a = 1 and b = 1) or (a = 2 and b = 3) or (a = 3 and b = 3)");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1).setValue("c", 1);
        ds.append().setValue("a", 2).setValue("b", 2).setValue("c", 2);
        ds.append().setValue("a", 3).setValue("b", 3).setValue("c", 3);
        ds.append().setValue("a", 4).setValue("b", 4).setValue("c", 4);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b","c"],[1,1,1],[3,3,3]]}""", ds.toString());
    }

    @Test
    public void test_bracket_3() {
        var obj = new SqlWhereFilter(
                "where ((a = 1 and b = 1) or (a = 2 and b = 2)) and (c = 2 and b = 2) or c is null");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1).setValue("c", 1);
        ds.append().setValue("a", 2).setValue("b", 2).setValue("c", 2);
        ds.append().setValue("a", 3).setValue("b", 3).setValue("c", 3);
        ds.append().setValue("a", 4).setValue("b", 4);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b","c"],[2,2,2],[4,4,null]]}""", ds.toString());
    }

    @Test
    public void test_bracket_4() {
        var obj = new SqlWhereFilter("where ((a = 1 and b = 1) or (a = 4 and c in (3,4)))");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1).setValue("c", 1);
        ds.append().setValue("a", 2).setValue("b", 2).setValue("c", 2);
        ds.append().setValue("a", 3).setValue("b", 3).setValue("c", 3);
        ds.append().setValue("a", 4).setValue("b", 4).setValue("c", 4);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b","c"],[1,1,1],[4,4,4]]}""", ds.toString());
    }

    @Test
    public void test_bracket_5() {
        var obj = new SqlWhereFilter("where a in (2,3,4) and (b = 1 or b = 3)");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1).setValue("c", 1);
        ds.append().setValue("a", 2).setValue("b", 2).setValue("c", 2);
        ds.append().setValue("a", 3).setValue("b", 3).setValue("c", 3);
        ds.append().setValue("a", 4).setValue("b", 4).setValue("c", 4);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b","c"],[3,3,3]]}""", ds.toString());
    }

    @Test
    public void test_bracket_6() {
        var obj = new SqlWhereFilter("where (b = 1 or b = 3) and a in (2,3,4)");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1).setValue("c", 1);
        ds.append().setValue("a", 2).setValue("b", 2).setValue("c", 2);
        ds.append().setValue("a", 3).setValue("b", 3).setValue("c", 3);
        ds.append().setValue("a", 4).setValue("b", 4).setValue("c", 4);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b","c"],[3,3,3]]}""", ds.toString());
    }

    @Test
    public void test_bracket_7() {
        var obj = new SqlWhereFilter("where (a = 1 and b = 1) or a in (2,3)");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1).setValue("c", 1);
        ds.append().setValue("a", 2).setValue("b", 2).setValue("c", 2);
        ds.append().setValue("a", 3).setValue("b", 3).setValue("c", 3);
        ds.append().setValue("a", 4).setValue("b", 4).setValue("c", 4);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b","c"],[1,1,1],[2,2,2],[3,3,3]]}""", ds.toString());
    }

    @Test
    public void test_bracket_8() {
        var obj = new SqlWhereFilter("where (b = 1 or b = 3) and a in (2,3,4) or (a = 1 and b = 2)");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1).setValue("c", 1);
        ds.append().setValue("a", 2).setValue("b", 2).setValue("c", 2);
        ds.append().setValue("a", 3).setValue("b", 3).setValue("c", 3);
        ds.append().setValue("a", 4).setValue("b", 4).setValue("c", 4);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b","c"],[3,3,3]]}""", ds.toString());
    }

    @Test
    public void test_bracket_9() {
        var obj = new SqlWhereFilter("where (b = 1 or b = 3) and (a in (2,3,4)) or (a = 1 and b = 2)");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1).setValue("c", 1);
        ds.append().setValue("a", 2).setValue("b", 2).setValue("c", 2);
        ds.append().setValue("a", 3).setValue("b", 3).setValue("c", 3);
        ds.append().setValue("a", 4).setValue("b", 4).setValue("c", 4);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b","c"],[3,3,3]]}""", ds.toString());
    }

    @Test
    public void test_bracket_10() {
        var obj = new SqlWhereFilter("where (b = 3) and (a in (2,3,4)) or (c = 2)");
        DataSet ds = new DataSet();
        ds.append().setValue("a", 1).setValue("b", 1).setValue("c", 1);
        ds.append().setValue("a", 2).setValue("b", 2).setValue("c", 2);
        ds.append().setValue("a", 3).setValue("b", 3).setValue("c", 3);
        ds.append().setValue("a", 4).setValue("b", 4).setValue("c", 4);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b","c"],[2,2,2],[3,3,3]]}""", ds.toString());
    }

    @Test
    public void test_bracket_str() {
        var obj = new SqlWhereFilter(
                "where (name in ('Jenny', 'Jack') and order_time_ is not null) or (age = 24 and name = 'Jackson')");
        DataSet ds = new DataSet();
        ds.append().setValue("name", "Jack").setValue("age", 23);
        ds.append().setValue("name", "Jackson").setValue("order_time_", "2023-08-01 15:08:41").setValue("age", 24);
        ds.append().setValue("name", "Jenny").setValue("order_time_", "2023-08-01 15:08:42").setValue("age", 25);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals(
                """
                        {"body":[["name","age","order_time_"],["Jackson",24,"2023-08-01 15:08:41"],["Jenny",25,"2023-08-01 15:08:42"]]}""",
                ds.toString());
    }

    public enum TestEnum {
        枚举0,
        枚举1,
        枚举2;
    }

    @Test
    public void test_enum() {
        var obj = new SqlWhereFilter("where a <> 1 and a <> 2");
        DataSet ds = new DataSet();
        ds.append().setValue("a", TestEnum.枚举1).setValue("b", 1);
        ds.append().setValue("a", TestEnum.枚举1).setValue("b", 2);
        ds.append().setValue("a", TestEnum.枚举2).setValue("b", 2);
        ds.append().setValue("a", TestEnum.枚举0).setValue("b", 3);
        ds.first();
        while (ds.fetch()) {
            if (!obj.pass(ds.current()))
                ds.delete();
        }
        assertEquals("""
                {"body":[["a","b"],[0,3]]}""", ds.toString());

    }

    @Test
    public void testSS() {
        String sql = "(A) or B and C and (D) and qwe";
        String subSql = sql.substring(sql.lastIndexOf(")") + 1);
        System.out.println(subSql);
        String firstConj = getFirstConj(subSql);
        String lastConj = getLastConj(subSql);
        subSql = subSql.substring(subSql.indexOf(firstConj) + firstConj.length() + 1);
        System.out.println(subSql);
        System.out.println(firstConj);
        System.out.println(lastConj);

    }

    // 获取第一个连词
    private String getFirstConj(String sql) {
        if (sql.contains("and ") || sql.contains("or ")) {
            int firstAndIndex = sql.toLowerCase().indexOf("and ");
            int firstOrIndex = sql.toLowerCase().indexOf("or ");
            if (firstAndIndex < 0)
                return "or";
            else if (firstOrIndex < 0)
                return "and";
            else
                return firstAndIndex < firstOrIndex ? "and" : "or";
        }
        return "";
    }

    // 获取最后一个连词
    private String getLastConj(String sql) {
        if (sql.contains("and ") || sql.contains("or ")) {
            int lastAndIndex = sql.lastIndexOf("and ");
            int lastOrIndex = sql.indexOf("or ");
            if (lastAndIndex < 0)
                return "or";
            else if (lastOrIndex < 0)
                return "and";
            else
                return lastAndIndex > lastOrIndex ? "and" : "or";
        }
        return "";
    }

}
