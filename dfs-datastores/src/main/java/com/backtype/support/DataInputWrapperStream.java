package com.backtype.support;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

public class DataInputWrapperStream extends InputStream {
    private DataInput input;

    public DataInputWrapperStream(DataInput input) {
        this.input = input;
    }

    @Override
    public int read() throws IOException {
        return input.readUnsignedByte();
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
       input.readFully(buf, off, len);
       return len;
    }
}
