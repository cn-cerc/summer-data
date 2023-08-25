package cn.cerc.db.core;

import javax.persistence.Column;
import javax.persistence.Table;

import org.springframework.context.annotation.Description;

@EntityKey(fields = { "code_" })
@Description("基类")
@Table(name = "test")
@SqlServer(type = SqlServerType.Pgsql)
public class StubEntity implements EntityImpl {
    @Column
    String code_;

    @Override
    public EntityHomeImpl getEntityHome() {
        return null;
    }

    @Override
    public void setEntityHome(EntityHomeImpl entityHome) {

    }
}
