package cn.cerc.local.tool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import cn.cerc.db.core.Utils;

public class XmlTool {
    private static final Logger log = LoggerFactory.getLogger(XmlTool.class);

    private static final XmlMapper mapper = createXmlMapper();

    private XmlTool() {
    }

    private static XmlMapper createXmlMapper() {
        return XmlMapper.builder()
                .defaultUseWrapper(false)
                .serializationInclusion(Include.NON_NULL) // 字段为 null，自动忽略，不再序列化
                .enable(MapperFeature.USE_STD_BEAN_NAMING)// 设置转换模式
                .build();
    }

    /**
     * 对象直接转换为 xml 的字符串
     */
    public static String toXml(Object value) throws JsonProcessingException {
        return toXml(value, null);
    }

    /**
     * 修改指定的根节点
     *
     * @param value 对象实体类
     * @param root  指定的根节点名称
     */
    public static String toXml(Object value, String root) throws JsonProcessingException {
        String xml;
        if (!Utils.isEmpty(root)) {
            xml = mapper.writer().withRootName(root).writeValueAsString(value);
        } else {
            xml = mapper.writeValueAsString(value);
        }

        log.debug(xml);
        return xml;
    }

    /**
     * 格式化xml字符串
     * 
     * @param xml  xml字符串
     * @param root 根节点名称
     */
    public static String formatXml(String xml, String root) {
        try {
            JsonNode node = mapper.readTree(xml);
            return mapper.writerWithDefaultPrettyPrinter().withRootName(root).writeValueAsString(node);
        } catch (JsonProcessingException e) {
            return xml;
        }
    }

}
