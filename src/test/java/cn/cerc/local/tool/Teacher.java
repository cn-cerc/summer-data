package cn.cerc.local.tool;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlText;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class Teacher {

    @JacksonXmlProperty(localName = "TypeCode")
    private TeacherType teacherTypeCode;

    @JacksonXmlProperty(localName = "Name")
    private String name;

    @JacksonXmlProperty(localName = "Gender")
    private String gender;

    @JacksonXmlProperty(localName = "Age")
    private String age;

    @Setter
    @Getter
    public static class TeacherType {

        @JacksonXmlProperty(isAttribute = true, localName = "type")
        private String type;

        @JacksonXmlText
        private String value;

        public TeacherType() {
        }

        public TeacherType(String type, String value) {
            this.type = type;
            this.value = value;
        }

    }

}
