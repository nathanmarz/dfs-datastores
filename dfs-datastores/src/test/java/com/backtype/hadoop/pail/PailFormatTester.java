package com.backtype.hadoop.pail;

import com.backtype.support.Utils;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import static com.backtype.support.TestUtils.*;
import static org.apache.hadoop.io.SequenceFile.Reader;
import static org.apache.hadoop.io.SequenceFile.Reader.file;
import static org.apache.hadoop.io.SequenceFile.Reader.start;


public abstract class PailFormatTester extends TestCase {
    PailFormat format;
    FileSystem local;

    public PailFormatTester() throws Exception {
        format = PailFormatFactory.create(getSpec());
        local = FileSystem.getLocal(new Configuration());
    }

    public void testInputFormat() throws Exception {
        String path = getTmpPath(local, "pail");
        Pail pail = Pail.create(local, path);
        Multimap<String, String> expected = HashMultimap.create();

        List<String> builder = new ArrayList<String>();
        for (int i = 0; i < Math.random() * 1000; i++) {
            String val = "a" + i;
            builder.add(val);
            expected.put("", val);
        }
        emitToPail(pail, "a", builder);

        builder = new ArrayList<String>();
        for (int i = 0; i < Math.random() * 1000000; i++) {
            String val = "b" + i;
            builder.add(val);
            expected.put("a/b/c/ddd", val);
        }
        emitToPail(pail, "a/b/c/ddd/1", builder);


        builder = new ArrayList<String>();
        for (int i = 0; i < Math.random() * 1000000; i++) {
            String val = "c" + i;
            builder.add(val);
            expected.put("a/b/d", val);
        }
        emitToPail(pail, "a/b/d/111", builder);

        Multimap<String, String> results = HashMultimap.create();


        InputFormat informat = format.getInputFormatClass().newInstance();
        JobConf conf = new JobConf();
        FileInputFormat.addInputPath(conf, new Path(path));
        InputSplit[] splits = informat.getSplits(conf, 10000);
        assertTrue(splits.length > 3); //want to test that splitting is working b/c i made really big files
        for (InputSplit split : splits) {
            RecordReader<PailRecordInfo, BytesWritable> rr = informat.getRecordReader(split, conf, Reporter.NULL);
            PailRecordInfo i = new PailRecordInfo();
            BytesWritable b = new BytesWritable();
            while (rr.next(i, b)) {
                results.put(i.getPailRelativePath(), new String(Utils.getBytes(b)));
            }
            rr.close();
        }
        assertEquals(expected, results);

        //TODO: test reading from a subbucket

    }

    public void testSplitOffsetAndRecordsToSkip() throws Exception {
        String path = getTmpPath(local, "splitOffset");
        Pail pail = Pail.create(local, path);
        Multimap<String, String> expected = HashMultimap.create();

        List<String> builder = Lists.newArrayList("1", "2", "3");
        emitToPail(pail, "a", builder);

        Multimap<String, String> results = HashMultimap.create();

        InputFormat informat = format.getInputFormatClass().newInstance();
        JobConf conf = new JobConf();
        FileInputFormat.addInputPath(conf, new Path(path));
        InputSplit[] splits = informat.getSplits(conf, 10000);
        assertTrue(splits.length == 1);
        InputSplit split = splits[0];
        RecordReader<PailRecordInfo, BytesWritable> rr = informat.getRecordReader(split, conf, Reporter.NULL);
        PailRecordInfo recordInfo = new PailRecordInfo();

        rr.next(recordInfo, new BytesWritable());
        assertDataFromRecordInfo(recordInfo, "1");

        rr.next(recordInfo, new BytesWritable());
        assertDataFromRecordInfo(recordInfo, "2");

        rr.next(recordInfo, new BytesWritable());
        assertDataFromRecordInfo(recordInfo, "3");
        rr.close();

        assertEquals(expected, results);
    }

    private void assertDataFromRecordInfo(PailRecordInfo recordInfo, String expected) throws IOException {
        Reader reader = new Reader(local.getConf(), file(new Path(recordInfo.getFullPath())), start(recordInfo.getSplitStartOffset()));
        BytesWritable value = new BytesWritable();
        for (int i = 0; i < recordInfo.getRecordsToSkip(); i++) reader.next(value);
        String actual = new String(value.getBytes());
        assertTrue(actual.equals(expected));
        reader.close();
    }


    protected abstract PailSpec getSpec();
}
