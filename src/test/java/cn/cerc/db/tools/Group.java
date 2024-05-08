package cn.cerc.db.tools;

import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

@JacksonXmlRootElement(localName = "Class")
public class Group {

    @JacksonXmlProperty(localName = "Teacher")
    private Teacher teacher; // 教师

    @JacksonXmlElementWrapper(localName = "Students")
    @JacksonXmlProperty(localName = "Stu")
    private List<Student> student; // 学生列表

    public Teacher getTeacher() {
        return teacher;
    }

    public void setTeacher(Teacher teacher) {
        this.teacher = teacher;
    }

    public List<Student> getStudent() {
        return student;
    }

    public void setStudent(List<Student> student) {
        this.student = student;
    }

}
