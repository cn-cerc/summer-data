package cn.cerc.core;

public class FastTime extends Datetime {
    private static final long serialVersionUID = -5117772213865216275L;

    public FastTime() {
        super();
        super.setDateKind(DateKind.OnlyTime);
        this.setEmptyDisplay(true);
        cutDate();
    }

    public FastTime(long date) {
        super(date);
        super.setDateKind(DateKind.OnlyTime);
        this.setEmptyDisplay(true);
        cutDate();
    }

    public FastTime(String date) {
        super(date);
        super.setDateKind(DateKind.OnlyTime);
        this.setEmptyDisplay(true);
        cutDate();
    }

    @Override
    public final FastTime setDateKind(DateKind dateKind) {
        throw new RuntimeException("disabled this operator");
    }

    public FastTime cutDate() {
        this.inc(DateType.Year, -this.get(DateType.Year) + 1);
        this.inc(DateType.Month, -this.get(DateType.Month) + 1);
        this.inc(DateType.Day, -this.get(DateType.Day) + 1);
        return this;
    }

    public FastTime incHour(int offset) {
        inc(DateType.Hour, offset);
        return this;
    }

    public FastTime incMinuteh(int offset) {
        inc(DateType.Minute, offset);
        return this;
    }

    public FastTime incSecond(int offset) {
        inc(DateType.Second, offset);
        return this;
    }

}
