package cn.cerc.db.core;

import java.util.Date;

@Deprecated
public class TDate extends TDateTime {
    private static final long serialVersionUID = -8262001955911346741L;

    public TDate(Date date) {
        super(date);
        this.setDateKind(DateKind.OnlyDate);
    }

    public TDate(long date) {
        super(date);
        this.setDateKind(DateKind.OnlyDate);
    }

    public TDate(String dateValue) {
        super(dateValue);
        this.setDateKind(DateKind.OnlyDate);
    }

    @Deprecated
    public static TDate Today() {
        return TDate.today();
    }

    @Deprecated
    public static TDate today() {
        TDate result = new TDate(System.currentTimeMillis());
        result.cut(DateType.Hour);
        return result;
    }

}
