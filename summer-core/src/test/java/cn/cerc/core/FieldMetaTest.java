package cn.cerc.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.gson.Gson;

import cn.cerc.db.editor.BooleanEditor;
import cn.cerc.db.editor.DatetimeEditor;
import cn.cerc.db.editor.FloatEditor;
import cn.cerc.db.editor.OptionEditor;

public class FieldMetaTest {

    @Test
    public void test_getset_diy() {
        DataRow rs = new DataRow();
        rs.fields().add("state").onGetText((record, meta) -> {
            return record.getInt(meta.code()) == 0 ? "停用" : "启用";
        }).onSetText(text -> {
            return "启用".equals(text) ? 1 : 0;
        });

        rs.setValue("state", 0);
        assertEquals("{\"state\":0}:停用", rs + ":" + rs.getText("state"));
        rs.setText("state", "启用");
        assertEquals("{\"state\":1}:启用", rs + ":" + rs.getText("state"));
        rs.setText("state", "停用");
        assertEquals("{\"state\":0}:停用", rs + ":" + rs.getText("state"));
    }

    @Test
    public void test_getset_bool() {
        DataRow rs = new DataRow();
        rs.fields().add("used").onGetSetText(new BooleanEditor("停用", "启用"));

        rs.setValue("used", false);
        assertEquals("{\"used\":false}:停用", rs + ":" + rs.getText("used"));
        rs.setText("used", "启用");
        assertEquals("{\"used\":true}:启用", rs + ":" + rs.getText("used"));
        rs.setText("used", "停用");
        assertEquals("{\"used\":false}:停用", rs + ":" + rs.getText("used"));
    }

    @Test
    public void test_getset_int() {
        DataRow rs = new DataRow();
        rs.fields().add("state").onGetSetText(new OptionEditor("停用", "启用"));

        rs.setValue("state", 0);
        assertEquals("{\"state\":0}:停用", rs + ":" + rs.getText("state"));
        rs.setText("state", "启用");
        assertEquals("{\"state\":1}:启用", rs + ":" + rs.getText("state"));
        rs.setText("state", "停用");
        assertEquals("{\"state\":0}:停用", rs + ":" + rs.getText("state"));
    }

    @Test
    public void test_getset_float() {
        DataRow rs = new DataRow();
        rs.fields().add("price").onGetSetText(new FloatEditor(2));

        rs.setValue("price", 1.06);
        assertEquals("1.06", rs.getText("price"));
        rs.setText("price", "1.3239");
        assertEquals("1.32", rs.getText("price"));
        rs.setText("price", "1.3");
        assertEquals("1.3", rs.getText("price"));

        rs.fields().add("price").onGetSetText(new FloatEditor(2, "0"));
        assertEquals("1.30", rs.getText("price"));
    }

    @Test
    public void test_getset_datetime() {
        String field = "date";
        DataRow rs = new DataRow();
        rs.fields().add(field).onGetSetText(new DatetimeEditor(Datetime.yyyyMM));
        rs.setValue(field, new Datetime("2021-12-09"));
        assertEquals("202112", rs.getText(field));
        rs.fields().get(field).onGetSetText(new DatetimeEditor(Datetime.HHmm));
        assertEquals("00:00", rs.getText(field));
    }

    @Test
    public void test_clone() {
        FieldMeta type1 = new FieldMeta("code");
        type1.dataType().readClass(String.class);
        type1.setRemark("abc");
        FieldMeta type2 = type1.clone();
        Gson gson = new Gson();
        assertEquals(gson.toJson(type1), gson.toJson(type2));
    }
}
