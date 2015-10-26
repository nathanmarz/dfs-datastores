package util;

import org.joda.time.format.DateTimeFormatterBuilder;

public class DateTimeFormatter {
    public static org.joda.time.format.DateTimeFormatter format() {
        return new DateTimeFormatterBuilder()
                .appendYear(4, 4)
                .appendMonthOfYear(2)
                .appendDayOfMonth(2)
                .toFormatter();
    }
}
