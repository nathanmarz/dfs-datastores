package com.backtype.hadoop.formats;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import static com.backtype.support.TestUtils.*;



public class SimpleStreamTest extends TestCase {
    public void testSimpleStreams() throws IOException {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        String path = getTmpPath(fs, "simplestreams");
        SimpleOutputStream sos = new SimpleOutputStream(new FileOutputStream(path));
        for(int i=0; i<=10000; i++) {
            sos.writeRaw(("prefix" + i + "suffix").getBytes());
        }
        sos.close();

        SimpleInputStream is = new SimpleInputStream(new FileInputStream(path));
        for(int i=0; i<=10000; i++) {
            assertArraysEqual(("prefix" + i + "suffix").getBytes(), is.readRawRecord());
        }
        assertNull(is.readRawRecord());
        assertNull(is.readRawRecord());
        assertNull(is.readRawRecord());
        assertNull(is.readRawRecord());
        assertNull(is.readRawRecord());
        is.close();
    }
}
