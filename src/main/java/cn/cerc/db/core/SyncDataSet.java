package cn.cerc.db.core;

public class SyncDataSet {
    private DataSet source;
    private DataSet target;
    private String keyFields;

    public SyncDataSet(DataSet source, DataSet target, String keyFields) {
        this.source = source;
        this.target = target;
        this.keyFields = keyFields;
    }

    public int execute(ISyncDataSet sync) throws ServiceException {
        int result = 0;
        source.first();
        while (source.fetch()) {
            result++;
            DataRow src = source.currentRow().get();
            String[] fields = keyFields.split(";");
            Object[] values = new Object[fields.length];
            for (int i = 0; i < fields.length; i++) {
                values[i] = src.getValue(fields[i]);
            }
            DataRow tar = target.locate(keyFields, values) ? target.currentRow().orElse(null) : null;
            sync.process(src, tar);
        }
        target.first();
        while (target.fetch()) {
            DataRow tar = target.currentRow().get();
            String[] fields = keyFields.split(";");
            Object[] values = new Object[fields.length];
            for (int i = 0; i < fields.length; i++) {
                values[i] = tar.getValue(fields[i]);
            }
            if (!source.locate(keyFields, values)) {
                result++;
                sync.process(null, tar);
            }
        }
        return result;
    }
}
