package cn.cerc.local.tool;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JacksonXMLTest {

    public static void main(String[] args) throws JsonProcessingException {
        XmlMapper xmlMapper = XmlMapper.builder()
                .defaultUseWrapper(false)
                .serializationInclusion(JsonInclude.Include.NON_NULL) // 字段为 null，自动忽略，不再序列化
                // .enable(SerializationFeature.INDENT_OUTPUT) // 格式化输出 xml
                .enable(MapperFeature.USE_STD_BEAN_NAMING) // 设置转换模式
                .build();
        // XML标签名:使用骆驼命名的属性名
        xmlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.UPPER_CAMEL_CASE);

        // 序列化 bean->xml
        Group group = new Group(); // 忍者班级

        Teacher teacher = new Teacher();
        teacher.setTeacherTypeCode(new Teacher.TeacherType("0", "严师"));
        teacher.setName("卡卡西");
        teacher.setAge("25");
        teacher.setGender("1");

        Student student1 = new Student();
        student1.setId("001"); // 学号
        student1.setName("鸣人");
        student1.setAge("18");
        student1.setGender("1");

        Student student2 = new Student();
        student2.setId("002"); // 学号
        student2.setName("佐助");
        student2.setAge("18");
        student2.setGender("1");

        Student student3 = new Student();
        student3.setId("003"); // 学号
        student3.setName("小樱");
        student3.setAge("18");
        student3.setGender("0");

        group.setTeacher(teacher);
        group.setStudent(Arrays.asList(student1, student2, student3));

        String result = xmlMapper.writeValueAsString(group);
        log.info(result);

        XmlMapper mapper = new XmlMapper();
        Group obj = mapper.readValue(result, Group.class);
        log.info(obj.getTeacher().getTeacherTypeCode().getValue());

        List<Student> items = obj.getStudent();
        items.forEach(k -> {
            log.info("姓名{} id{} 年龄{}", k.getName(), k.getId(), k.getAge());
        });

    }
}
