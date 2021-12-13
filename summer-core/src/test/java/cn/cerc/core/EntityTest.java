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
    @Column(length = 100)
    private String remark_;
    @Column
    @Version
    private Integer version_;
}
