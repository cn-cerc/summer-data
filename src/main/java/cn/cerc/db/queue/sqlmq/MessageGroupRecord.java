package cn.cerc.db.queue.sqlmq;

public record MessageGroupRecord(String groupCode, boolean executeStatus, int total, int doneNum) {

}