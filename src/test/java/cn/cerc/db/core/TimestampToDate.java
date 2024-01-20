package cn.cerc.db.core;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TimestampToDate {
    public static void main(String[] args) {
        // 使用 Instant 创建 Date 和 LocalDateTime
        Date date = new Date(-62135625943000L);

        Instant instant = Instant.ofEpochMilli(-62135625943000L);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

        // 将 LocalDateTime 转换为 UTC 时区的 Date 对象
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
        Date formDate = Date.from(zonedDateTime.toInstant());

        // 比较 Date 和 LocalDateTime 的日期和时间
        System.out.println("Date: " + date);
        System.out.println("LocalDateTime: " + localDateTime);
        // 定义时间格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        System.out.println("LocalDateTime Format: " + localDateTime.format(formatter));

        System.out.println("zonedDateTime: " + zonedDateTime);

        System.out.println("Date from LocalDateTime: " + formDate);
        System.out.println(new Datetime(-62135625943000L));

        System.out.println("Datetime zero：" + Datetime.zero().asBaseDate());
    }
}