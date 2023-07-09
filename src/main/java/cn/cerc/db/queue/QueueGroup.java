package cn.cerc.db.queue;

public class QueueGroup {

    private String code;
    private int row = 1;
    private int column = 0;

    public QueueGroup(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }

    public int incrRow() {
        if (column == 0)
            throw new RuntimeException("当前行没有列数，不得进行下一行");
        column = 1;
      return  ++row;
    }

    public int row() {
        return row;
    }

    public int incrColumn() {
        return ++column;
    }

    public int column() {
        return column;
    }

}
