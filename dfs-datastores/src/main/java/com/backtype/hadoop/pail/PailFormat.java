package com.backtype.hadoop.pail;

import org.apache.hadoop.mapred.InputFormat;

import com.backtype.hadoop.formats.RecordStreamFactory;

public interface PailFormat extends RecordStreamFactory {
    public Class<? extends InputFormat> getInputFormatClass();
}
