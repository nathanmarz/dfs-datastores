package backtype.hadoop.pail;

import backtype.hadoop.formats.RecordStreamFactory;
import org.apache.hadoop.mapred.InputFormat;

public interface PailFormat extends RecordStreamFactory {
    public Class<? extends InputFormat> getInputFormatClass();
}
