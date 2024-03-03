package cn.cerc.db.core;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.gson.annotations.Expose;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class Handle implements IHandle {

    @Expose(serialize = false, deserialize = false)
    private ISession session;

    public Handle() {

    }

    public Handle(ISession session) {
        this.session = session;
    }

    public Handle(IHandle handle) {
        if (handle != null)
            this.session = handle.getSession();
    }

    @Override
    public void setSession(ISession session) {
        this.session = session;
    }

    @Override
    public ISession getSession() {
        return session;
    }

    /**
     * 仅用于单元测试
     */
    public static IHandle getStub() {
        return new Handle(new StubSession());
    }
}
