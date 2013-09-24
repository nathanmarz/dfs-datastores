package com.backtype.support;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.backtype.hadoop.formats.RecordInputStream;
import com.backtype.hadoop.formats.RecordOutputStream;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.google.common.collect.TreeMultiset;

public class TestUtils {

    private static final String TMP_ROOT = "/tmp/unittests/"+UUID.randomUUID().toString();

    public static void assertArraysEqual(byte[] expected, byte[] result) {
        if (!Arrays.equals(expected, result)) {
            throw new AssertionError("Arrays not equal");
        }
    }

    public static void assertNull(Object val) {
        if(val!=null) {
            throw new AssertionError("val is not null");
        }
    }

    public static void assertNotNull(Object val) {
        if(val==null) {
            throw new AssertionError("val is null");
        }
    }

    public static String getTmpPath(FileSystem fs, String name) throws IOException {
        fs.mkdirs(new Path(TMP_ROOT));
        String full = TMP_ROOT + "/" + name;
        if (fs.exists(new Path(full))) {
            fs.delete(new Path(full), true);
        }
        return full;
    }

    public static void deletePath(FileSystem fs, String path) throws IOException {
        fs.delete(new Path(path), true);
    }

    public static void emitToPail(Pail pail, String file, Iterable<String> records) throws IOException {
        RecordOutputStream os = pail.openWrite(file);
        for (String s: records) {
            os.writeRaw(s.getBytes());
        }
        os.close();
    }

    public static void emitToPail(Pail pail, String file, String... records) throws IOException {
        RecordOutputStream os = pail.openWrite(file);
        for (String s: records) {
            os.writeRaw(s.getBytes());
        }
        os.close();
    }

    public static void emitObjectsToPail(Pail pail, Object... records) throws IOException {
        TypedRecordOutputStream os = pail.openWrite();
        for(Object r: records) {
            os.writeObject(r);
        }
        os.close();
    }

    public static void emitObjectsToPail(Pail pail, List records) throws IOException {
        TypedRecordOutputStream os = pail.openWrite();
        for(Object r: records) {
            os.writeObject(r);
        }
        os.close();
    }


    public static List<String> getPailRecords(Pail pail) throws IOException {
        List<String> ret = new ArrayList<String>();
        for(String s: pail.getUserFileNames()) {
            RecordInputStream is = pail.openRead(s);
            while(true) {
                byte[] r = is.readRawRecord();
                if(r==null) break;
                ret.add(new String(r));
            }
            is.close();
        }
        return ret;
    }

    public static <T> void assertPailContents(Pail<T> pail, T... objects) {
        TreeMultiset contains = getPailContents(pail);
        TreeMultiset other = TreeMultiset.create();
        for(T obj: objects) {
            other.add(obj);
        }
        Assert.assertEquals(other, contains, failureString(other, contains));
    }

    public static String failureString(Iterable expected, Iterable got) {
        String ret = "\n\nExpected:\n";
        for(Object o: expected) {
            ret = ret + o.toString() + "\n\n";
        }
        ret+="\nGot\n";
        for(Object o: got) {
            ret = ret + o.toString() + "\n\n";
        }
        ret+="\n\n";
        return ret;
    }

    public static void assertPailContents(Pail pail, List objects) {
        TreeMultiset contains = getPailContents(pail);
        TreeMultiset other = TreeMultiset.create();
        for(Object obj: objects) {
            other.add(obj);
        }
        for(Object o: contains) {

        }
        Assert.assertEquals(other, contains, failureString(other, contains));
    }


    public static <T> TreeMultiset<T> getPailContents(Pail<T> pail) {
        TreeMultiset contains = TreeMultiset.create();
        for(T obj: pail) {
            contains.add(obj);
        }
        return contains;
    }
}
