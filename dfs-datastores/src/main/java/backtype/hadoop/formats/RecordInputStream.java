package backtype.hadoop.formats;

import java.io.IOException;

public interface RecordInputStream {
    //return null at end
    public byte[] readRawRecord() throws IOException;
    public void close() throws IOException;
}
