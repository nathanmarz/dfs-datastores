package com.backtype.hadoop.pail;

import com.backtype.hadoop.formats.RecordStreamFactory;
import org.apache.hadoop.mapred.InputFormat;

public interface PailFormat extends RecordStreamFactory {
    public Class<? extends InputFormat> getInputFormatClass();
}
