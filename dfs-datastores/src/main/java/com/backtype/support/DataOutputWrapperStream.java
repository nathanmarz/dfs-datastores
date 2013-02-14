package com.backtype.support;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

public class DataOutputWrapperStream extends OutputStream {
    DataOutput output;

    public DataOutputWrapperStream(DataOutput output) {
        this.output = output;
    }

    @Override
    public void write(int b) throws IOException {
        output.writeByte(b);
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
        output.write(buf, off, len);
    }

}
