package util;

import org.joda.time.DateTime;

public class DateHelper {
    public DateHelper() {
    }

    public static String weekInterval(DateTime var0) {
        int var1 = var0.getDayOfWeek();
        String var2 = var0.minusDays(var1 - 1).toString("YYYYMMdd");
        String var3 = var0.plusDays(7 - var1).toString("YYYYMMdd");
        return String.format(var2 + "_" + var3, new Object[0]);
    }
}

