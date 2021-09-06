package cn.cerc.core;

public class FastDate extends Datetime {
    private static final long serialVersionUID = -5117772213865216275L;

    public FastDate() {
        super();
        super.setDateKind(DateKind.OnlyDate);
        this.setEmptyDisplay(true);
        cutTime();
    }

    public FastDate(long date) {
        super(date);
        super.setDateKind(DateKind.OnlyDate);
        this.setEmptyDisplay(true);
        cutTime();
    }

    public FastDate(String date) {
        super(date);
        super.setDateKind(DateKind.OnlyDate);
        this.setEmptyDisplay(true);
        cutTime();
    }

    @Override
    public final FastDate setDateKind(DateKind dateKind) {
        throw new RuntimeException("disabled this operator");
    }

    public FastDate cutTime() {
        this.cut(DateType.Hour);
        return this;
    }
    
    public FastDate incYear(int offset) {
        inc(DateType.Year, offset);
        return this;
    }
    
    public FastDate incMonth(int offset) {
        inc(DateType.Month, offset);
        return this;
    }
    
    public FastDate incDay(int offset) {
        inc(DateType.Day, offset);
        return this;
    }

}
