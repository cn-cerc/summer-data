package cn.cerc.db.mysql;

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
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import cn.cerc.db.core.DataRow;
import cn.cerc.db.core.DataSet;
import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.Describe;
import cn.cerc.db.core.FastDate;
import cn.cerc.db.core.IHandle;
import cn.cerc.db.core.ISession;
import cn.cerc.db.core.ISqlDatabase;
import cn.cerc.db.core.Utils;

public class MysqlDatabase implements IHandle, ISqlDatabase {
    public static final String DefaultOID = "UID_";
    private Class<?> clazz;
    private ISession session;

    public MysqlDatabase(IHandle handle, Class<?> clazz) {
        super();
        this.clazz = clazz;
        if (handle != null)
            this.setSession(handle.getSession());
    }

    @Override
    public final String table() {
        return Utils.findTable(clazz);
    }

    @Override
    public String oid() {
        return Utils.findOid(clazz, DefaultOID);
    }

    @Override
    public boolean createTable(boolean overwrite) {
        MysqlServerMaster server = this.getMysql();
        List<String> list = server.tables(this);
        String table = table();
        if (!list.contains(table.toLowerCase()))
            server.execute(getCreateSql());
        return true;
    }

    public boolean createEntityClass(String table) {
        MysqlServerMaster server = (MysqlServerMaster) this.getSession().getProperty(MysqlServerMaster.SessionId);
        String dataBase = server.getDatabase();
        String filePath = ".\\src\\main\\java\\" + this.clazz.getPackageName().replaceAll("\\.", "\\\\");

        MysqlQuery ds = new MysqlQuery(this);
        ds.add("select table_name,table_comment from information_schema.tables where table_schema='%s'", dataBase);
        ds.add("and table_name='%s'", table);
        ds.openReadonly();
        while (ds.fetch()) {
            String tableName = ds.getString("table_name");
            String tableComment = ds.getString("table_comment");
            String fileName = tableName.substring(0, 1).toUpperCase() + tableName.substring(1);
            try {
                File file = new File(filePath + "\\" + fileName + ".java");
                file.createNewFile();
                BufferedWriter writer = new BufferedWriter(new FileWriter(file));
                writer.write(String.format("package %s;\r\n\r\n", this.clazz.getPackageName()));
                writer.write("import javax.persistence.*;\r\n");
                writer.write("import org.springframework.stereotype.Component;\r\n");
                writer.write("import cn.cerc.*;\r\n");
                writer.write("import lombok.Getter;\r\n");
                writer.write("import lombok.Setter;\r\n");
                writer.write("@Component\r\n");
                writer.write("@Entity\r\n");
                String tableIndex = getTableIndex(tableName);
                writer.write(tableIndex);
                writer.write("@SqlServer(type = SqlServerType.Mysql)\r\n");
                writer.write("@Getter\r\n");
                writer.write("@Setter\r\n");
                if (!Utils.isEmpty(tableComment)) {
                    writer.write(String.format("@Describe(name = \"%s\")\r\n", tableComment));
                }
                writer.write(String.format("public class %s extends AdoTable {\r\n\r\n", fileName));

                MysqlQuery dsColumn = new MysqlQuery(this);
                dsColumn.add("select column_name,data_type,column_type,extra,is_nullable,column_comment,");
                dsColumn.add("column_default from information_schema.columns");
                dsColumn.add("where table_schema='%s' and table_name='%s'", dataBase, tableName);
                dsColumn.openReadonly();
                while (dsColumn.fetch()) {
                    String extra = dsColumn.getString("extra");
                    if ("auto_increment".equals(extra)) {
                        writer.write("@Id\r\n");
                        writer.write("@GeneratedValue\r\n");
                    }
                    String field = dsColumn.getString("column_name");
                    String codeComment = dsColumn.getString("column_comment");
                    String nullable = dsColumn.getString("is_nullable");
                    String dataType = dsColumn.getString("data_type");
                    // 转换为Java的数据类型
                    String codeType = getType(dataType);
                    String columnType = dsColumn.getString("column_type");
                    if ("datetime".equals(dataType) || "text".equals(dataType) || "ntext".equals(dataType)
                            || "mediumtext".equals(dataType) || "longtext".equals(dataType)
                            || "timestamp".equals(dataType) || "blob".equals(dataType)) {
                        writer.write("@Column(");
                        if (!"YES".equals(nullable)) {
                            writer.write("nullable = false, ");
                        }
                        writer.write(String.format("columnDefinition = \"%s\"", dataType));
                        writer.write(")\r\n");
                    } else {
                        String codeLength = columnType.substring(columnType.indexOf("(") + 1, columnType.indexOf(")"));
                        StringBuilder strColumn = new StringBuilder();
                        strColumn.append("@Column(");
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
                        writer.write(strColumn.toString());
                    }
                    writer.write(String.format("@Describe(name = \"%s\"", codeComment));
                    String def = dsColumn.getString("column_default");
                    if (!Utils.isEmpty(def)) {
                        writer.write(String.format(", def = \"%s\"", def));
                    }
                    writer.write(")\r\n");
                    writer.write(String.format("private %s %s;\r\n\r\n", codeType, field));
                    // 把缓存区内容压入文件
                    writer.flush();
                }
                writer.write("}");
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
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
        MysqlQuery ds = new MysqlQuery(this);
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
        if (table != null && table.indexes().length > 0) {
            sb.append(",");
            Index[] indexs = table.indexes();
            count = 0;
            for (Index index : indexs) {
                if (count++ > 0) {
                    sb.append(",");
                }
                sb.append("\n");
                if ("PRIMARY".equals(index.name())) {
                    sb.append("primary key (").append(index.columnList()).append(")");
                    continue;
                }
                if (index.unique()) {
                    sb.append("unique ");
                }
                sb.append("index ").append(index.name()).append(" (").append(index.columnList()).append(")");
            }
        } else {
            String fields = "";
            for (Field field : clazz.getDeclaredFields()) {
                Id id = field.getDeclaredAnnotation(Id.class);
                if (id != null) {
                    fields = fields + field.getName() + ",";
                }
            }
            if (Utils.isEmpty(fields))
                throw new RuntimeException("lack primary key");
            sb.append(",").append("\n").append("primary key (").append(fields.substring(0, fields.length() - 1))
                    .append(")");
        }
        sb.append("\n) engine=innodb charset=utf8 collate=utf8_general_ci;");
        return sb.toString();
    }

    private void writeDataType(StringBuffer sb, Field field) {
        Column column = field.getDeclaredAnnotation(Column.class);
        if (field.getType() == String.class) {
            int size = 255;
            if (column != null)
                size = column.length();
            if (!Utils.isEmpty(column.columnDefinition()) && "text".equals(column.columnDefinition())) {
                sb.append("text");
            } else {
                sb.append("varchar(").append(size).append(")");
            }
        } else if (field.getType().isEnum() || field.getType() == int.class || field.getType() == Integer.class
                || field.getType() == long.class || field.getType() == Long.class) {
            int size = 11;
            if (column != null)
                size = column.length();
            sb.append("int(").append(size).append(")");
        } else if (Datetime.class.isAssignableFrom(field.getType())
                || FastDate.class.isAssignableFrom(field.getType())) {
            sb.append("datetime");
        } else if (field.getType() == double.class || field.getType() == Double.class) {
            int precision = 18;
            int scale = 4;
            if (column != null) {
                precision = column.precision();
                scale = column.scale();
            }
            sb.append("decimal(").append(precision).append(",").append(scale).append(")");
        } else if (field.getType() == boolean.class || field.getType() == Boolean.class) {
            sb.append("bit(").append(1).append(")");
        } else {
            throw new RuntimeException("不支持的类型：" + field.getType().getName());
        }
        Describe des = field.getDeclaredAnnotation(Describe.class);
        if (des != null && !Utils.isEmpty(des.def())) {
            sb.append(" default ").append(des.def());
        }
        Id id = field.getDeclaredAnnotation(Id.class);
        if ((column != null) && (!column.nullable()))
            sb.append(" not null");
        else if (id != null)
            sb.append(" not null");
        else
            sb.append(" null");
        GeneratedValue gen = field.getDeclaredAnnotation(GeneratedValue.class);
        if (gen != null) {
            sb.append(" auto_increment");
        }
        if (des != null) {
            sb.append(" comment '").append(Utils.isEmpty(des.name()) ? field.getName() : des.name()).append("'");
        }
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
