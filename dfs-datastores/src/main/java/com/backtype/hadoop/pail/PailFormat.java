package com.backtype.hadoop.pail;

import com.backtype.hadoop.formats.RecordStreamFactory;
import org.apache.hadoop.mapred.InputFormat;

import java.util.Map;

public interface PailFormat extends RecordStreamFactory {
    public void configure(Map<String, Object> args);
    public Class<? extends InputFormat> getInputFormatClass();
}
