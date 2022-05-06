package cn.cerc.db.mssql;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.Describe;
import cn.cerc.db.core.EntityHelper;
import cn.cerc.db.core.EntityImpl;
import cn.cerc.db.core.FastDate;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.ISqlDatabase;
import cn.cerc.db.core.Utils;

public class MssqlDatabase implements IHandle, ISqlDatabase {
    public static final String DefaultOID = "UpdateKey_";
    private Class<? extends EntityImpl> clazz;
    private EntityHelper<? extends EntityImpl> helper;
    private ISession session;

    public MssqlDatabase(IHandle handle, Class<? extends EntityImpl> clazz) {
        super();
        this.clazz = clazz;
        this.helper = EntityHelper.create(clazz);
        if (handle != null)
            this.setSession(handle.getSession());
    }

    @Override
    public final String table() {
        return helper.table();
    }

    @Override
    public final String oid() {
        return helper.idFieldCode();
    }

    @Override
    public boolean createTable(boolean overwrite) {
        MssqlServer server = (MssqlServer) this.getSession().getProperty(MssqlServer.SessionId);
        List<String> list = server.tables(this);
        String table = table();
        if (!list.contains(table))
            server.execute(getCreateSql());
        return true;
    }

    public boolean createEntityClass(String table) {
        String filePath = ".\\src\\main\\java\\" + this.clazz.getPackageName().replaceAll("\\.", "\\\\");

        MssqlQuery ds = new MssqlQuery(this);
        ds.add("select name from sys.tables where type='U' and name='%s'", table);
        ds.open();
        while (ds.fetch()) {
            String tableName = ds.getString("name");
            String fileName = tableName.substring(0, 1).toUpperCase() + tableName.substring(1);
            try {
                File file = new File(filePath + "\\" + fileName + ".java");
                file.createNewFile();
                BufferedWriter writer = new BufferedWriter(new FileWriter(file));
                writer.write(String.format("package %s;\r\n\r\n", this.clazz.getPackageName()));
                writer.write("import javax.persistence.*;\n");
                writer.write("import org.springframework.stereotype.Component;\n");
                writer.write("import cn.cerc.*;\n");
                writer.write("import lombok.Getter;\n");
                writer.write("import lombok.Setter;\n");
                writer.write("@Component\n");
                writer.write("@Entity\n");
                String tableIndex = getTableIndex(tableName);
                writer.write(tableIndex);
                writer.write("@SqlServer(type = SqlServerType.Mssql)\n");
                writer.write("@Getter\n");
                writer.write("@Setter\n");
                writer.write("@Permission(Permission.GUEST)\n");
                writer.write(String.format("public class %s extends AdoTable {\n\n", fileName));
                // 获取列
                MssqlQuery cdsTmp = new MssqlQuery(this);
                cdsTmp.add("select Field_=a.name,FieldType_=b.name,");
                cdsTmp.add("AutoIncreatement_=case when columnproperty(a.id,a.name,'IsIdentity')=1");
                cdsTmp.add("then 'true' else 'false' end,");
                cdsTmp.add("PrimaryKey_=case when exists(select 1 from sysobjects where xtype='PK' and name in(");
                cdsTmp.add("select name from sysindexes where indid in(select indid from sysindexkeys");
                cdsTmp.add("where id = a.id and colid=a.colid))) then 'true' else 'false' end,");
                cdsTmp.add("FieldLength_=columnproperty(a.id,a.name,'PRECISION'),");
                cdsTmp.add("Scale_=isnull(columnproperty(a.id,a.name,'Scale'),0),");
                cdsTmp.add("NullAble_=case when a.isnullable=1 then 'true' else 'false' end,");
                cdsTmp.add("DefValue_=isnull(e.text,''),Comment_=isnull(CONVERT(varchar(200), g.[value]),'')");
                cdsTmp.add("from syscolumns a");
                cdsTmp.add("left join systypes b on a.xtype=b.xusertype");
                cdsTmp.add("inner join sysobjects d on a.id=d.id and d.xtype='U' and d.name<>'dtproperties'");
                cdsTmp.add("left join syscomments e on a.cdefault=e.id");
                cdsTmp.add("left join sys.extended_properties g on a.id=g.major_id and a.colid=g.minor_id");
                cdsTmp.add("left join sys.extended_properties f on d.id=f.major_id and f.minor_id =0");
                cdsTmp.add("where d.name='%s'", tableName);
                cdsTmp.open();
                boolean hasPrimary = false;
                while (cdsTmp.fetch()) {
                    boolean isPrimary = cdsTmp.getBoolean("PrimaryKey_");
                    // 先只生成一个主键
                    if (isPrimary && !hasPrimary) {
                        writer.write("@Id\n");
                        hasPrimary = true;
                    }
                    boolean autoIncreatement = cdsTmp.getBoolean("AutoIncreatement_");
                    if (autoIncreatement) {
                        writer.write("@GeneratedValue\n");
                    }
                    String field = cdsTmp.getString("Field_");
                    String comment = cdsTmp.getString("Comment_");
                    boolean nullable = cdsTmp.getBoolean("NullAble_");
                    String dataType = cdsTmp.getString("FieldType_");
                    // 转换为Java的数据类型
                    String codeType = getType(dataType);
                    if ("datetime".equals(dataType) || "text".equals(dataType) || "ntext".equals(dataType)) {
                        writer.write("@Column(");
                        if (!nullable) {
                            writer.write("nullable = false, ");
                        }
                        writer.write(String.format("columnDefinition = \"%s\"", dataType));
                        writer.write(")\n");
                    } else {
                        String fieldLength = cdsTmp.getString("FieldLength_");
                        if (dataType.equals("uniqueidentifier")) {
                            fieldLength = "38";
                        }
                        StringBuilder strColumn = new StringBuilder();
                        strColumn.append("@Column(");
                        int scale = cdsTmp.getInt("Scale_");
                        if (scale != 0) {
                            strColumn.append(String.format("precision = %s, scale = %s", fieldLength, scale));
                        } else {
                            strColumn.append(String.format("length = %s", fieldLength));
                        }
                        if (!nullable) {
                            strColumn.append(", nullable = false");
                        }
                        if (dataType.equals("uniqueidentifier")) {
                            strColumn.append(", columnDefinition = \"uniqueidentifier\"");
                        } else if (dataType.equals("tinyint")) {
                            strColumn.append(", columnDefinition = \"tinyint\"");
                        } else if (dataType.equals("smallint")) {
                            strColumn.append(", columnDefinition = \"smallint\"");
                        }
                        strColumn.append(")\n");
                        writer.write(strColumn.toString());
                    }
                    String def = cdsTmp.getString("DefValue_");
                    writer.write(String.format("@Describe(name = \"%s\"", comment));
                    if (!Utils.isEmpty(def)) {
                        if (dataType.equals("uniqueidentifier")) {
                            def = "newid()";
                        } else {
                            def = def.replaceAll("\\(", "").replaceAll("\\)", "");
                        }
                        writer.write(String.format(", def = \"%s\"", def));
                    }
                    writer.write(")\n");
                    writer.write(String.format("private %s %s;\n\n", codeType, field));
                    // 把缓存区内容压入文件
                    writer.flush();
                }
                writer.write("}");
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    private String getType(String dataType) {
        String type = "String";
        switch (dataType) {
        case "nvarchar":
        case "text":
        case "ntext":
        case "uniqueidentifier":
            type = "String";
            break;
        case "smallint":
        case "tinyint":
        case "int":
            type = "Integer";
            break;
        case "bigint":
            type = "Long";
            break;
        case "numeric":
            type = "Double";
            break;
        case "float":
            type = "Float";
            break;
        case "bit":
            type = "Boolean";
            break;
        case "datetime":
            type = "Datetime";
            break;
        default:
            break;
        }
        return type;
    }

    private String getTableIndex(String tableName) {
        MssqlQuery ds = new MssqlQuery(this);
        ds.add("select c.name as ix_name,c.is_unique as ix_unique,e.name as column_name,c.is_primary_key");
        ds.add("from sys.tables a");
        ds.add("inner join sys.indexes c on a.object_id=c.object_id");
        ds.add("inner join sys.index_columns d on d.object_id=c.object_id and d.index_id=c.index_id");
        ds.add("inner join sys.columns e on e.object_id=d.object_id and e.column_id=d.column_id");
        ds.add("where a.name='%s'", tableName);
        ds.open();
        // 读取全部数据再保存
        Map<String, DataSet> items = new LinkedHashMap<>();
        String oldKeyName = "";
        boolean hasPrimary = false;
        while (ds.fetch()) {
            String keyName = ds.getString("ix_name");
            DataSet dataIn = items.get(keyName);
            if (dataIn == null && !Utils.isEmpty(keyName)) {
                dataIn = new DataSet();
                dataIn.head().copyValues(ds.current(), "ix_unique", "ix_name", "is_primary_key");
                items.put(keyName, dataIn);
                oldKeyName = keyName;
                if (ds.getBoolean("is_primary_key"))
                    hasPrimary = true;
            } else {
                dataIn = items.get(oldKeyName);
            }
            dataIn.append();
            dataIn.setValue("column_name", ds.getString("column_name"));
        }
        StringBuilder strTable = new StringBuilder();
        strTable.append(String.format("@Table(name = \"%s\"", tableName));
        if (hasPrimary) {
            strTable.append(", uniqueConstraints = @UniqueConstraint(");
            for (String key : items.keySet()) {
                DataSet data = items.get(key);
                DataRow record = data.head();
                String keyName = record.getString("ix_name");
                boolean isPrimary = record.getBoolean("is_primary_key");
                if (isPrimary) {
                    strTable.append(String.format("name = \"%s\", columnNames = {", keyName));
                    while (data.fetch()) {
                        strTable.append(String.format("\"%s\"", data.getString("column_name")));
                        if (data.recNo() < data.size()) {
                            strTable.append(",");
                        }
                    }
                    strTable.append("})");
                }
            }
        }
        if (!items.isEmpty()) {
            strTable.append(", indexes = {");
        }
        int i = 0;
        for (String key : items.keySet()) {
            DataSet data = items.get(key);
            DataRow record = data.head();
            int non_unique = record.getInt("ix_unique");
            String keyName = record.getString("ix_name");
            boolean isPrimary = record.getBoolean("is_primary_key");
            if (isPrimary) {
                continue;
            }
            String fields = "";
            while (data.fetch()) {
                fields = fields + data.getString("column_name") + ",";
            }
            if (i > 0) {
                strTable.append(",");
            }
            strTable.append(String.format("@Index(name = \"%s\", columnList = \"%s\"", keyName,
                    fields.substring(0, fields.length() - 1)));
            if (non_unique == 0) {
                strTable.append(", unique = true)");
            } else {
                strTable.append(")");
            }
            i++;
        }
        if (!items.isEmpty()) {
            strTable.append("}");
        }
        strTable.append(")\n");
        return strTable.toString();
    }

    public String getCreateSql() {
        StringBuffer sb = new StringBuffer();
        sb.append("create table ").append(table()).append(" (");
        int count = 0;
        for (Field field : clazz.getDeclaredFields()) {
            if (count++ > 0)
                sb.append(",");
            sb.append("\n");
            sb.append(field.getName()).append(" ");
            writeDataType(sb, field);
        }
        Table table = clazz.getDeclaredAnnotation(Table.class);
        if (table != null && table.uniqueConstraints().length > 0) {
            sb.append(",\n");
            UniqueConstraint[] uniques = table.uniqueConstraints();
            for (UniqueConstraint unique : uniques) {
                sb.append(String.format("constraint %s primary key clustered (\n", unique.name()));
                count = 0;
                for (String column : unique.columnNames()) {
                    if (count++ > 0)
                        sb.append(",");
                    sb.append(column).append("\n");
                }
                sb.append(
                        ")with (pad_index=off,statistics_norecompute=off,ignore_dup_key=off,allow_row_locks=on,allow_page_locks=on) on [primary]\n");
                sb.append(") on [primary]");
            }
        } else {
            sb.append("\n)");
        }
        if (table != null && table.indexes().length > 0) {
            sb.append("\ngo\n");
            Index[] indexs = table.indexes();
            for (Index index : indexs) {
                sb.append("create");
                if (index.unique()) {
                    sb.append(" unique");
                }
                sb.append(" nonclustered index ").append(index.name()).append(" on ").append(table()).append("(")
                        .append(index.columnList()).append(")").append(" on [primary]").append("\n").append("go\n");
            }
        }
        return sb.toString();
    }

    private void writeDataType(StringBuffer sb, Field field) {
        Column column = field.getDeclaredAnnotation(Column.class);
        if (field.getType() == String.class) {
            int size = 255;
            if (column != null)
                size = column.length();
            if (!Utils.isEmpty(column.columnDefinition())) {
                sb.append(column.columnDefinition());
            } else {
                sb.append("nvarchar(").append(size).append(")");
            }
        } else if (field.getType().isEnum() || field.getType() == Integer.class) {
            if ("tinyint".equals(column.columnDefinition()) || "smallint".equals(column.columnDefinition())) {
                sb.append(column.columnDefinition());
            } else {
                sb.append("int");
            }
        } else if (Datetime.class.isAssignableFrom(field.getType())
                || FastDate.class.isAssignableFrom(field.getType())) {
            sb.append("datetime");
        } else if (field.getType() == Double.class) {
            int precision = 18;
            int scale = 4;
            if (column != null) {
                precision = column.precision();
                scale = column.scale();
            }
            sb.append("numeric(").append(precision).append(",").append(scale).append(")");
        } else if (field.getType() == Float.class) {
            sb.append("float");
        } else if (field.getType() == Boolean.class) {
            sb.append("bit");
        } else {
            throw new RuntimeException("不支持的类型：" + field.getType().getName());
        }
        Describe des = field.getDeclaredAnnotation(Describe.class);
        GeneratedValue gen = field.getDeclaredAnnotation(GeneratedValue.class);
        if (gen != null)
            sb.append(" identity(1,1)");
        if (des != null && !Utils.isEmpty(des.def())) {
            sb.append(" default ").append(des.def());
        }
        if ((column != null) && (!column.nullable()))
            sb.append(" not null");
        else
            sb.append(" null");
    }

    @Override
    public ISession getSession() {
        return session;
    }

    @Override
    public void setSession(ISession session) {
        this.session = session;
    }

}
