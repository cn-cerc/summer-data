package cn.cerc.db.mysql;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.Utils;

public class MysqlEntityBuilder {

    private IHandle handle;

    public MysqlEntityBuilder(IHandle handle) {
        this.handle = handle;
    }

    public boolean createEntityClass(String table, Class<?> clazz) throws IOException {
        if (Utils.isEmpty(table))
            throw new RuntimeException("database table can not be empty");

        var config = MysqlConfig.getMaster();
        MysqlQuery query = new MysqlQuery(handle);
        query.add("select table_name,table_comment from information_schema.tables");
        query.addWhere().eq("table_schema", config.database()).eq("table_name", table).build();
        query.openReadonly();

        while (query.fetch()) {
            String tableName = query.getString("table_name");
            String tableComment = query.getString("table_comment");

            StringBuilder builder = new StringBuilder();
            builder.append(String.format("package %s;\r\n\r\n", clazz.getPackageName()));
            builder.append("import javax.persistence.*;\r\n");
            builder.append("import org.springframework.stereotype.Component;\r\n");
            builder.append("import cn.cerc.*;\r\n");
            builder.append("\r\n");
            builder.append("@Component\r\n");
            builder.append("@Entity\r\n");

            String tableIndex = getTableIndex(tableName);
            builder.append(tableIndex);
            builder.append("@SqlServer(type = SqlServerType.Mysql)\r\n");
            if (!Utils.isEmpty(tableComment))
                builder.append(String.format("@Description(\"%s\")\r\n", tableComment));
            builder.append(String.format("public class %s extends CustomEntity {\r\n\r\n", clazz.getSimpleName()));

            MysqlQuery dsColumn = new MysqlQuery(handle);
            dsColumn.add("select column_name,data_type,column_type,extra,is_nullable,column_comment,");
            dsColumn.add("column_default from information_schema.columns");
            dsColumn.add("where table_schema='%s' and table_name='%s'", config.database(), tableName);
            dsColumn.openReadonly();
            while (dsColumn.fetch()) {
                String extra = dsColumn.getString("extra");
                if ("auto_increment".equals(extra)) {
                    builder.append("@Id\r\n");
                    builder.append("@GeneratedValue\r\n");
                }
                String field = dsColumn.getString("column_name");
                String codeComment = dsColumn.getString("column_comment");
                String nullable = dsColumn.getString("is_nullable");
                String dataType = dsColumn.getString("data_type");
                // 转换为Java的数据类型
                String codeType = getType(dataType);
                String columnType = dsColumn.getString("column_type");
                if ("datetime".equals(dataType) || "text".equals(dataType) || "ntext".equals(dataType)
                        || "mediumtext".equals(dataType) || "longtext".equals(dataType) || "timestamp".equals(dataType)
                        || "blob".equals(dataType)) {
                    builder.append("@Column(");
                    builder.append(String.format("name = \"%s\",", codeComment));
                    if (!"YES".equals(nullable)) {
                        builder.append("nullable = false, ");
                    }
                    builder.append(String.format("columnDefinition = \"%s\"", dataType));
                    builder.append(")\r\n");
                } else {
                    String codeLength = columnType.substring(columnType.indexOf("(") + 1, columnType.indexOf(")"));
                    StringBuilder strColumn = new StringBuilder();
                    if ("version_".equalsIgnoreCase(field)) {
                        strColumn.append("@Version\r\n");
                    }
                    strColumn.append("@Column(");
                    strColumn.append(String.format("name = \"%s\",", codeComment));
                    if (codeLength.contains(",")) {
                        strColumn.append(String.format("precision = %s, scale = %s", codeLength.split(",")[0],
                                codeLength.split(",")[1]));
                    } else {
                        strColumn.append(String.format("length = %s", codeLength));
                    }
                    if (!"YES".equals(nullable)) {
                        strColumn.append(", nullable = false");
                    }
                    strColumn.append(")\r\n");
                    builder.append(strColumn.toString());
                }
                String def = dsColumn.getString("column_default");
                if (!Utils.isEmpty(def)) {
                    builder.append(String.format(", def = \"%s\"", def));
                }
                builder.append(String.format("private %s %s;\r\n\r\n", codeType, field));
            }
            builder.append("}");

//            Path path = Paths.get("/", fileName + ".java");
//            if (!Files.exists(path))
//                Files.createDirectory(path.getParent());
//            Files.write(path, builder.toString().getBytes(StandardCharsets.UTF_8));
            System.out.println(builder.toString());
        }
        return true;
    }

    private String getType(String dataType) {
        String type = "String";
        switch (dataType) {
        case "varchar":
        case "text":
        case "ntext":
        case "mediumtext":
        case "longtext":
        case "blob":
            type = "String";
            break;
        case "int":
            type = "Integer";
            break;
        case "bigint":
            type = "Long";
            break;
        case "decimal":
        case "float":
            type = "Double";
            break;
        case "bit":
            type = "Boolean";
            break;
        case "datetime":
        case "timestamp":
            type = "Datetime";
            break;
        default:
            break;
        }
        return type;
    }

    private String getTableIndex(String tableName) {
        MysqlQuery ds = new MysqlQuery(handle);
        ds.add("show index from %s", tableName);
        ds.openReadonly();
        // 读取全部数据再保存
        Map<String, DataSet> items = new LinkedHashMap<>();
        String oldKeyName = "";
        while (ds.fetch()) {
            String keyName = ds.getString("Key_name");
            DataSet dataIn = items.get(keyName);
            if (dataIn == null && !Utils.isEmpty(keyName)) {
                dataIn = new DataSet();
                dataIn.head().copyValues(ds.current(), "Non_unique", "Key_name");
                items.put(keyName, dataIn);
                oldKeyName = keyName;
            } else {
                dataIn = items.get(oldKeyName);
            }
            dataIn.append();
            dataIn.setValue("Column_name", ds.getString("Column_name"));
            dataIn.setValue("Collation", ds.getString("Collation"));
        }
        StringBuilder strTable = new StringBuilder();
        strTable.append(String.format("@Table(name = \"%s\"", tableName));
        if (!items.isEmpty()) {
            strTable.append(", indexes = {");
        }
        int i = 0;
        for (String key : items.keySet()) {
            DataSet data = items.get(key);
            DataRow record = data.head();
            int non_unique = record.getInt("Non_unique");
            String keyName = record.getString("Key_name");
            String fields = "";
            while (data.fetch()) {
                fields = fields + data.getString("Column_name") + ",";
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
        strTable.append(")\r\n");
        return strTable.toString();
    }

}
