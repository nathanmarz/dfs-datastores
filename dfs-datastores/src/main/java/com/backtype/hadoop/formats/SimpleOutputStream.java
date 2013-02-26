package com.backtype.hadoop.formats;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;


public class SimpleOutputStream implements RecordOutputStream {

    private OutputStream _raw;
    private DataOutputStream _os;

    public SimpleOutputStream(OutputStream os) {
        _raw = os;
        _os = new DataOutputStream(os);
    }

    public OutputStream getWrappedStream() {
        return _raw;
    }

    public void writeRaw(byte[] record) throws IOException {
        writeRaw(record, 0, record.length);
    }

    public void writeRaw(byte[] record, int start, int length) throws IOException {
        _os.writeInt(length);
        _os.write(record, start, length);
    }

    public void close() throws IOException {
        _os.close();
    }
}
