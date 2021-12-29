package cn.cerc.db.core;

public class DateFormatErrorException extends Exception {
    public DateFormatErrorException(String text) {
        super(String.format("error data: %s", text));
    }

    private static final long serialVersionUID = 4487684309718505100L;

}
