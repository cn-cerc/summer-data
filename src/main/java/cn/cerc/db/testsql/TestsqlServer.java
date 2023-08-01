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
    private Map<String, DataSet> tables = new HashMap<>();
    private static TestsqlServer server;
    private static boolean Enabled;

    public static TestsqlServer build() {
        TestsqlServer.Enabled = true;
        if (server == null)
            server = new TestsqlServer();
        return server;
    }

    /**
     * 请改使用 build
     * 
     * @param b
     * @return
     */
    public static TestsqlServer get(boolean value) {
        return build();
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

    public void select(DataSet dataSet, String table, String sql) {
        if (!tables.containsKey(table))
            tables.put(table, dataSet);
        var consumer = this.onSelect.get(table);
        if (consumer != null)
            consumer.accept(dataSet, sql);
        for (var row : dataSet) {
            row.setState(DataRowState.None);
            for (var field : dataSet.fields())
                field.setKind(FieldKind.Storage);
        }
    }

    public void onSelect(String table, ConsumerTable consumer) {
        this.onSelect.put(table, consumer);
    }

    public static boolean enabled() {
        return Enabled;
    }

    public Map<String, DataSet> tables() {
        return tables;
    }

}
