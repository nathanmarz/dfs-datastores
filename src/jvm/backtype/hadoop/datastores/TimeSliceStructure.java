package backtype.hadoop.datastores;

import backtype.hadoop.pail.PailStructure;
import backtype.support.Utils;
import java.util.Collections;
import java.util.List;


public abstract class TimeSliceStructure<T> implements PailStructure<T> {

    public final boolean isValidTarget(String... dirs) {
        if(dirs.length < 2) {
            return false;
        }
        String weekTimeStr = dirs[0];
        String sliceTimeStr = dirs[1];
        try {
            int weekTimeSecs = Integer.parseInt(weekTimeStr);
            int sliceTimeSecs = Integer.parseInt(sliceTimeStr);

            int week = Utils.toWeek(weekTimeSecs);

            int weekStart = Utils.weekStartTime(week);
            int weekEnd = Utils.weekStartTime(week+1);

            return weekStart == weekTimeSecs && sliceTimeSecs >= weekStart && sliceTimeSecs < weekEnd;
        } catch(NumberFormatException nfe) {
            return false;
        }
    }

    public final List<String> getTarget(Object object) {
        return Collections.EMPTY_LIST; // this isn't valid. At write time, a valid category must be provided
    }

}
