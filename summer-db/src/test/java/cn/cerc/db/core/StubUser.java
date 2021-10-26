package cn.cerc.db.core;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import cn.cerc.db.core.ClassFactory;
import cn.cerc.db.core.SearchKey;

@Entity(name = "s_user")
public class StubUser {

    @Id
    @Column(name = "ID_")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @SearchKey
    @Column(name = "code_")
    private String code;

    @Column(name = "name_")
    private String name;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static void main(String[] args) {
        ClassFactory.printDebugInfo(StubUser.class);
    }
}
