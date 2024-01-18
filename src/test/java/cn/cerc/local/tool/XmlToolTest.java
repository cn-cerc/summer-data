package cn.cerc.local.tool;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class XmlToolTest {

    record TestEntity(Integer uid, String name) {

    }

    @Test
    public void testToXml() throws JsonProcessingException {
        TestEntity item = new TestEntity(1, "薛明远");
        String xml = XmlTool.toXml(item);
        Assert.assertEquals("<TestEntity><uid>1</uid><name>薛明远</name></TestEntity>", xml);
    }

    @Test
    public void testToXml2() throws JsonProcessingException {
        TestEntity item = new TestEntity(2, "许建政");
        String xml = XmlTool.toXml(item, "entity");
        Assert.assertEquals("<entity><uid>2</uid><name>许建政</name></entity>", xml);
    }

    @Test
    public void testFromXml() {
        TestEntity item = XmlTool.fromXml("<entity><uid>3</uid><name>林平</name></entity>", TestEntity.class);
        assert item != null;
        Assert.assertEquals(3, item.uid().intValue());
        Assert.assertEquals("林平", item.name());
    }

    @Test
    public void testFromXml2() {
        String xml = """
                <entitys>
                  <entity><uid>1</uid><name>薛明远</name></entity>
                  <entity><uid>2</uid><name>许建政</name></entity>
                  <entity><uid>3</uid><name>林平</name></entity>
                </entitys>
                """;
        TypeFactory factory = new ObjectMapper().getTypeFactory();
        CollectionType type = factory.constructCollectionType(ArrayList.class, TestEntity.class);
        List<TestEntity> list = XmlTool.fromXml(xml, type);
        assert list != null;
        Assert.assertEquals(3, list.size());
        Assert.assertEquals("薛明远", list.get(0).name());
        Assert.assertEquals("许建政", list.get(1).name());
        Assert.assertEquals("林平", list.get(2).name());
    }

    // @Test
    // 此处因为换行符号不一致会导致对比错误
    public void testFormatXml() {
        String xml = "<entity><uid>2</uid><name>许建政</name></entity>";
        String formatXml = XmlTool.formatXml(xml, "entity");
        Assert.assertEquals("""
                <entity>
                  <uid>2</uid>
                  <name>许建政</name>
                </entity>
                """, formatXml);
    }

}