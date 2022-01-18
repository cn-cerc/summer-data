package cn.cerc.db.dao;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Version;

import cn.cerc.db.core.EntityHelper;
import cn.cerc.db.core.EntityHomeImpl;
import cn.cerc.db.core.EntityImpl;

@Entity
public class UserTest implements EntityImpl {
    @Id
    public String id_;
    @Column(name = "Code_")
    public String code;
    @Column(name = "Name_")
    public String name;
    @Column(name = "Mobile_")
    public String mobile;
    @Version
    public Integer version_;

    public static void main(String[] args) {
        EntityHelper<UserTest> meta = EntityHelper.create(UserTest.class);
        System.out.println(meta);
        System.out.println(meta.table());
        System.out.println(meta.idField());
        System.out.println(meta.versionField());
        System.out.println(meta.idFieldCode());
    }

    @Override
    public EntityHomeImpl getEntityHome() {
        return null;
    }

    @Override
    public void setEntityHome(EntityHomeImpl entityHome) {

    }
}