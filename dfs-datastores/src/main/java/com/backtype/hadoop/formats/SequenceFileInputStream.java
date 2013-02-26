package com.backtype.hadoop.formats;

import com.backtype.support.Utils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

public class SequenceFileInputStream implements RecordInputStream {

    private SequenceFile.Reader _reader;
    private BytesWritable writable = new BytesWritable();

    public SequenceFileInputStream(FileSystem fs, Path path) throws IOException {
        _reader = new SequenceFile.Reader(fs, path, fs.getConf());
    }

    public byte[] readRawRecord() throws IOException {
        boolean gotnew = _reader.next(writable, NullWritable.get());
        if (!gotnew) {
            return null;
        }
        return Utils.getBytes(writable);
    }

    public void close() throws IOException {
        _reader.close();
    }
}
