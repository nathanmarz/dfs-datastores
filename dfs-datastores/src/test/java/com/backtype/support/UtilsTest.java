package com.backtype.support;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import static backtype.support.TestUtils.*;

public class UtilsTest extends TestCase {
    public void testGetBytes() {
        BytesWritable b = new BytesWritable(new byte[] {1, 2, 3});
        assertArraysEqual(new byte[] {1, 2, 3}, Utils.getBytes(b));
        b.set(new byte[] {1}, 0, 1);
        assertArraysEqual(new byte[] {1}, Utils.getBytes(b));
    }

    public void testJoin() {
        assertEquals("a/b/ccc/d", Utils.join(new String[] { "a", "b", "ccc", "d"}, "/"));
        assertEquals("1", Utils.join(new String[] {"1"}, "/asdads"));

        List<Integer> args = new ArrayList<Integer>() {{
            add(1);
            add(2);
            add(10);
        }};
        assertEquals("1, 2, 10", Utils.join(args, ", "));
    }

    public void testFill() throws Exception {
        ByteArrayInputStream is = new ByteArrayInputStream(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        byte[] buf = new byte[10];
        assertEquals(8, Utils.fill(is, buf));
        for(int i=1; i<=8; i++) {
            assertEquals(i, buf[i-1]);
        }
        is = new ByteArrayInputStream(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        buf = new byte[4];
        assertEquals(4, Utils.fill(is, buf));
        for(int i=1; i<=4; i++) {
            assertEquals(i, buf[i-1]);
        }
    }

    public void testFirstNBytesSame() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        String path1 = getTmpPath(fs, "file1");
        String path2 = getTmpPath(fs, "file2");

        FSDataOutputStream os = fs.create(new Path(path1));
        os.write(new byte[] {1, 2, 3, 4, 10, 11, 12});
        os.close();

        os = fs.create(new Path(path2));
        os.write(new byte[] {1, 2, 3, 4, 5, 6});
        os.close();

        assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 3));
        assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 4));
        assertFalse(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 5));
        assertFalse(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 1000));
    }

    public void testFirstNBytesFileLength() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        String path1 = getTmpPath(fs, "file1");
        String path2 = getTmpPath(fs, "file2");

        FSDataOutputStream os = fs.create(new Path(path1));
        os.write(new byte[] {1, 2, 2, 3, 3});
        os.close();

        os = fs.create(new Path(path2));
        os.write(new byte[] {1, 2, 2, 3, 3, 4});
        os.close();
        assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 3));
        assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 4));
        assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 5));
        assertFalse(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 1000));

        path2 = getTmpPath(fs, "file2");
        os = fs.create(new Path(path2));
        os.write(new byte[] {1, 2, 2, 3, 3});
        os.close();
        assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 3));
        assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 4));
        assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 5));
        assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 1000));


    }
}
