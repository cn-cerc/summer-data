package cn.cerc.db.testsql;

import java.util.HashMap;
import java.util.Map;

import cn.cerc.db.core.DataRowState;
import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.FieldMeta.FieldKind;
import cn.cerc.db.core.ISqlServer;
import cn.cerc.db.core.ServerClient;

public class TestsqlServer implements ISqlServer {
    public static final String DefaultOID = "UID_";
    private Map<String, ConsumerTable> onSelect = new HashMap<>();
    private static TestsqlServer server;

    public static TestsqlServer get() {
        if (server == null)
            server = new TestsqlServer();
        return server;
    }

    @Override
    public ServerClient getClient() {
        return new TestsqlClient();
    }

    @Override
    public boolean execute(String sql) {
        return false;
    }

    @Override
    public String getHost() {
        return "memory";
    }

    public void select(DataSet query, String table, String sql) {
        var consumer = this.onSelect.get(table);
        if (consumer != null)
            consumer.accept(query, sql);
        for (var row : query) {
            row.setState(DataRowState.None);
            for (var field : query.fields())
                field.setKind(FieldKind.Storage);
        }
    }

    public void onSelect(String table, ConsumerTable consumer) {
        this.onSelect.put(table, consumer);
    }

}
