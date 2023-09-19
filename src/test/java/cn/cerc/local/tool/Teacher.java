package cn.cerc.local.tool;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlText;

public class Teacher {

    @JacksonXmlProperty(localName = "TypeCode")
    private TeacherType teacherTypeCode;

    @JacksonXmlProperty(localName = "Name")
    private String name;

    @JacksonXmlProperty(localName = "Gender")
    private String gender;

    @JacksonXmlProperty(localName = "Age")
    private String age;

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

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

    }

    public TeacherType getTeacherTypeCode() {
        return teacherTypeCode;
    }

    public void setTeacherTypeCode(TeacherType teacherTypeCode) {
        this.teacherTypeCode = teacherTypeCode;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

}
