package cn.cerc.core;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Version;

@Entity
@SqlServer
public class EntityTest {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Integer id_;
    @Column(length = 30, nullable = false)
    private String code_;
    @Column(length = 50, nullable = false)
    private String name_;
    @Column
    private TypeEnum Type_;
    @Column(length = 100)
    private String remark_;
    @Column
    @Version
    private Integer version_;
    
    public enum TypeEnum {
        V1, V2;
    }
    public static void main(String[] args) {
        FieldDefs defs = new FieldDefs(EntityTest.class);
        DataRow row = new DataRow(defs);
        row.setValue("Type_", 1);
        System.out.println(row.getValue("Type_"));
        System.out.println(row);
        EntityTest entity = row.asEntity(EntityTest.class);
        System.out.println(entity.Type_);
        System.out.println();
    }
}
