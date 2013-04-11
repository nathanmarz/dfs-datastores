package com.backtype.hadoop.datastores;

import com.backtype.support.FSTestCase;
import junit.framework.Assert;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;


import static com.backtype.support.TestUtils.getTmpPath;

public class VersionedStoreTest extends FSTestCase {

    public void testCleanup() throws Exception{
        String tmp1 = getTmpPath(fs, "versions");
        VersionedStore vs = new VersionedStore(tmp1);
        for (int i = 1; i <= 4; i ++) {
            String version = vs.createVersion(i);
            fs.mkdirs(new Path(version));
            vs.succeedVersion(i);
        }
        FileStatus[] files = fs.listStatus(new Path(tmp1));
        Assert.assertEquals(files.length, 8);
        vs.cleanup(2);
        files = fs.listStatus(new Path(tmp1));
        Assert.assertEquals(files.length, 4);
        for (FileStatus f : files) {
            String path = f.getPath().toString();
            Assert.assertTrue(path.endsWith("3") ||
            path.endsWith("4") ||
            path.endsWith("3.version") ||
            path.endsWith(("4.version")));
        }
    }

}
