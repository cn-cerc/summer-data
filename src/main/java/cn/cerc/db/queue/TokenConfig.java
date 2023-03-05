package cn.cerc.db.queue;

import cn.cerc.db.core.ISession;

public class TokenConfig implements TokenConfigImpl {
    private String token;
    private String original;
    private ISession session;

    public TokenConfig() {
    }

    public TokenConfig(String token) {
        this.token = token;
    }

    @Override
    public String getToken() {
        return token;
    }

    @Override
    public ISession getSession() {
        return session;
    }

    @Override
    public void setSession(ISession session) {
        this.session = session;
    }

    @Override
    public String getOriginal() {
        return original;
    }

    public void setOriginal(String original) {
        this.original = original;
    }

}
