package util;

import org.joda.time.DateTime;

public class DateHelper {
    public DateHelper() {
    }

    public static String weekInterval(DateTime date) {
        int day = date.getDayOfWeek();
        String startDate = date.minusDays(day - 1).toString("YYYYMMdd");
        String endDate = date.plusDays(7 - day).toString("YYYYMMdd");
        return String.format(startDate + "_" + endDate, new Object[0]);
    }
}

