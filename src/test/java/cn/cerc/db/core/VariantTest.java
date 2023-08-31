package cn.cerc.db.core;

import static org.junit.Assert.assertEquals;

import javax.persistence.Column;

import org.junit.Test;

public class VariantTest {

    public enum TestEnum {
        枚举A,
        枚举B;
    }

    @Strict(false)
    public static class TestEntity implements EntityImpl {
        @Column
        Integer number1;
        @Column
        int number2;
        @Column
        TestEnum testEnum1;
        @Column
        TestEnum testEnum2;

        @Override
        public EntityHomeImpl getEntityHome() {
            return null;
        }

        @Override
        public void setEntityHome(EntityHomeImpl entityHome) {
        }

    }

    @Test
    public void test() {
        Variant tmp = new Variant("value").setKey("key");
        assertEquals(tmp.key(), "key");

        assertEquals(tmp.getString(), "value");
    }

    @Test
    public void test_writeToEntity() {
        DataRow dataRow = new DataRow();
        dataRow.setValue("number1", null).setValue("number2", null).setValue("testEnum2", 1);
        TestEntity entity = dataRow.asEntity(TestEntity.class);
        assertEquals(null, entity.number1);
        assertEquals(2, entity.number2);
        assertEquals(null, entity.testEnum1);
        assertEquals(TestEnum.枚举B, entity.testEnum2);
    }

}
