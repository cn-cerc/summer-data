package cn.cerc.db.testsql;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

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
    public void test11() {
        String s = "abc111abcd";
        System.out.println(s.split("or").length);
    }

    @Test
    public void testpick() {
        String s = "(A or B) and (B and (C or D))";
        pickBracket(s);
    }

    class JudgeTree {
        // 不是and就是or
        private boolean isAnd;
        private String items;
        private JudgeTree left;
        private JudgeTree right;

        public boolean isAnd() {
            return isAnd;
        }

        public void setAnd(boolean isAnd) {
            this.isAnd = isAnd;
        }

        public String getItems() {
            return items;
        }

        public void setItems(String items) {
            this.items = items;
        }

        public JudgeTree getLeft() {
            return left;
        }

        public void setLeft(JudgeTree left) {
            this.left = left;
        }

        public JudgeTree getRight() {
            return right;
        }

        public void setRight(JudgeTree right) {
            this.right = right;
        }

    }

    public void pickBracket(String s) {
        Stack<Integer> stack = new Stack<>();
        int index = 0;
        int lastRight = 0;
        List<String> items = new ArrayList<>();
        List<String> symbols = new ArrayList<>();
        List<Integer> splitIndex = new ArrayList<>();
        while (index < s.length()) {
            if (s.charAt(index) == '(') {
                stack.push(index);
                if (lastRight != 0) {
                    String middle = s.substring(lastRight, index);
                    if (middle.contains("and")) {
                        symbols.add("and");
                        splitIndex.add(s.indexOf("and", lastRight));
                    } else if (middle.contains("or")) {
                        symbols.add("or");
                        splitIndex.add(s.indexOf("or", lastRight));
                    }
                    lastRight = 0;
                }
            }
            if (s.charAt(index) == ')') {
                if (stack.isEmpty()) {
                    throw new RuntimeException("括号格式不规范！");
                } else {
                    if (stack.size() == 1) {
                        items.add(s.substring(stack.pop() + 1, index));
                        lastRight = index;
                    } else {
                        stack.pop();
                    }

                }
            }
            index++;
        }
        for (String temp : items) {
            System.out.println(temp);
        }

        for (String temp : symbols) {
            System.out.println(temp);
        }
    }

}
