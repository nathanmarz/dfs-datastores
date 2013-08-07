package com.backtype.hadoop.formats;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public interface RecordStreamFactory extends Serializable {
    public RecordInputStream getInputStream(FileSystem fs, Path path) throws IOException;
    public RecordOutputStream getOutputStream(FileSystem fs, Path path) throws IOException;
}
