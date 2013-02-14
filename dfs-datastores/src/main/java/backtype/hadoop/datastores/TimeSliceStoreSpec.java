package backtype.hadoop.datastores;

import backtype.hadoop.pail.PailSpec;

import java.io.Serializable;
import java.util.Map;

public class TimeSliceStoreSpec implements Serializable {
    private PailSpec _pailSpec;

    public TimeSliceStoreSpec() {
        this(null, null);
    }

    public TimeSliceStoreSpec(TimeSliceStructure structure) {
        this(null, null, structure);
    }

    public TimeSliceStoreSpec(String format, Map<String, Object> args) {
        this(format, args, new DefaultTimeSliceStructure());
    }

    public TimeSliceStoreSpec(String format, Map<String, Object> args, TimeSliceStructure structure) {
        _pailSpec = new PailSpec(format, args, structure);
    }


    public PailSpec toPailSpec() {
        return _pailSpec;
    }
}
