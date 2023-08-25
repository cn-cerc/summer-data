package cn.cerc.db.core;

import javax.persistence.Column;

import org.springframework.context.annotation.Description;

@EntityKey(fields = { "code_", "name_" })
@Description("子类")
@SqlServer(type = SqlServerType.Mysql)
public class StubChildEntity extends StubEntity {
    @Column
    String name_;
}
