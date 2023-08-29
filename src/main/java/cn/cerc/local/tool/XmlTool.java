package cn.cerc.local.tool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import cn.cerc.db.core.Utils;

public class XmlTool {
    private static final Logger log = LoggerFactory.getLogger(XmlTool.class);

    public static String ROOT_ELEMENT_RESPONSE = "response";

    /**
     * 对象直接转换为 xml 的字符串
     */
    public static <T> String toXml(T value) throws JsonProcessingException {
        return toXml(value, null);
    }

    /**
     * 修改指定的根节点
     *
     * @param value 对象实体类
     * @param root  指定的根节点名称
     */
    public static <T> String toXml(T value, String root) throws JsonProcessingException {
        XmlMapper mapper = XmlMapper.builder()
                .defaultUseWrapper(false)
                .serializationInclusion(JsonInclude.Include.NON_NULL) // 字段为 null，自动忽略，不再序列化
                // .enable(SerializationFeature.INDENT_OUTPUT) // 格式化输出 xml
                .enable(MapperFeature.USE_STD_BEAN_NAMING) // 设置转换模式
                .build();

        String xml;
        if (!Utils.isEmpty(root)) {
            xml = mapper.writer().withRootName(root).writeValueAsString(value);
        } else {
            xml = mapper.writeValueAsString(value);
        }

        log.info(xml);
        return xml;
    }

}
