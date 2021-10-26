package cn.cerc.db.core;

import java.time.LocalDateTime;

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
        LocalDateTime ldt = this.asLocalDateTime();
        ldt = ldt.plusYears(1 - ldt.getYear());
        ldt = ldt.plusMonths(1 - ldt.getMonthValue());
        ldt = ldt.plusDays(1 - ldt.getDayOfMonth());
        this.setTimestamp(ldt.atZone(LocalZone).toInstant().toEpochMilli());
        return this;
    }

}
