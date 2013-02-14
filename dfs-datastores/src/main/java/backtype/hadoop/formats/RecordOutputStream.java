package backtype.hadoop.formats;

import java.io.IOException;


public interface RecordOutputStream {
    public void writeRaw(byte[] record) throws IOException;
    public void writeRaw(byte[] record, int start, int length) throws IOException;
    public void close() throws IOException;
}
