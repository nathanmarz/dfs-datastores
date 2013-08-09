package com.backtype.hadoop.formats;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;


public class SimpleInputStream implements RecordInputStream {

    DataInputStream _in;

    public SimpleInputStream(InputStream in) {
        _in = new DataInputStream(in);
    }

    public byte[] readRawRecord() throws IOException {
        try {
            int size = _in.readInt();
            byte[] ret = new byte[size];
            _in.readFully(ret);
            return ret;
        } catch(EOFException e) {
            return null;
        }
    }

    public void close() throws IOException {
        _in.close();
    }
}
