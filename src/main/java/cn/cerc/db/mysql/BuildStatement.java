package cn.cerc.db.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import cn.cerc.db.core.Datetime;
import cn.cerc.db.core.Utils;

/**
 * 生成原生sql操作对象
 *
 * @author 张弓
 */
public class BuildStatement implements AutoCloseable {
    private Connection connection;
    private StringBuilder build = new StringBuilder();
    private PreparedStatement ps = null;
    private List<Object> items = new ArrayList<>();
    private SimpleDateFormat sdf;

    public BuildStatement(Connection connection) {
        this.connection = connection;
    }

    public BuildStatement append(String sql) {
        build.append(sql);
        return this;
    }

    public void append(String sql, Object data) {
        build.append(sql);
        Object result = data;

        // 转换
        if (data instanceof Datetime) {
            result = data.toString();
        } else if (data instanceof LocalDateTime localDateTime) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            result = localDateTime.format(formatter);
        } else if (data instanceof Double) {
            result = Utils.roundTo((Double) data, -8);
        } else {
            if (data instanceof Date) {
                if (sdf == null) {
                    sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                }
                result = sdf.format(data);
            }
        }
        items.add(result);
    }

    public PreparedStatement build() throws SQLException {
        if (ps != null) {
            throw new RuntimeException("ps not is null");
        }

        ps = connection.prepareStatement(build.toString());

        // sql占位符赋值，从1开始
        int i = 0;
        for (Object value : items) {
            i++;
            ps.setObject(i, value);
        }
        return ps;
    }

    public String getCommand() {
        if (ps == null) {
            return null;
        }

        String result = ps.toString();
        return result.substring(result.indexOf(':') + 2);
    }

    public String getPrepareCommand() {
        return build.toString();
    }

    @Override
    public void close() {
        try {
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
