package cn.cerc.db.core;

import java.util.HashMap;
import java.util.Map;

public class StubSession implements ISession {
    private Map<String, Object> items = new HashMap<String, Object>();

    public StubSession() {

    }

    @Override
    public Object getProperty(String key) {
        return items.get(key);
    }

    @Override
    public void setProperty(String key, Object value) {
        items.put(key, value);
    }

    @Override
    public boolean loadToken(String token) {
        return true;
    }

    @Override
    public void close() {

    }

    public StubSession setIndustry(String industry) {
        this.setProperty(ISession.INDUSTRY, industry);
        return this;
    }

    public StubSession setCorpNo(String corpNo) {
        this.setProperty(ISession.CORP_NO, corpNo);
        return this;
    }

    public StubSession setUserCode(String userCode) {
        this.setProperty(ISession.USER_CODE, userCode);
        return this;
    }

    public StubSession setUserName(String userName) {
        this.setProperty(ISession.USER_NAME, userName);
        return this;
    }

}
