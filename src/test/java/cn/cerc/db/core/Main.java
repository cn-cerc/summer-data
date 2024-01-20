package cn.cerc.db.core;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class Main {
    public static void main(String[] args) {
        // 时间字符串--数据库存储的日期读取时用 LocalDateTime 存值，最终也要转换为 Date 的 Sun Jan 02 23:54:17 CST 1
//        String dateTimeString = "0001-01-02 23:54:17";
        String dateTimeString = new Datetime().toString();

        // 定义时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 将字符串转换为 LocalDateTime
        LocalDateTime localDateTime = LocalDateTime.parse(dateTimeString, formatter);

        System.out.println("DateTime String: " + dateTimeString);
        System.out.println("LocalDateTime: " + localDateTime);

        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
        Date date = Date.from(zonedDateTime.toInstant());
        System.out.println("Date: " + date);// Tue Jan 04 23:48:34 CST 1

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String result = sdf.format(date);
        System.out.println(result);
    }
}