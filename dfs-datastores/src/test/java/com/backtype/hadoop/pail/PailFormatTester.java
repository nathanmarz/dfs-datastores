package com.backtype.hadoop.pail;

import com.backtype.support.Utils;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import com.backtype.hadoop.mapred.lib.CombineFileInputFormat;

import java.util.ArrayList;
import java.util.List;

import static com.backtype.support.TestUtils.emitToPail;
import static com.backtype.support.TestUtils.getTmpPath;


public abstract class PailFormatTester extends TestCase {
    PailFormat format;
    FileSystem local;

    public PailFormatTester() throws Exception{
        format = PailFormatFactory.create(getSpec());
        local = FileSystem.getLocal(new Configuration());
    }

    public void testInputFormat() throws Exception {
        String path = getTmpPath(local, "pail");
        Pail pail = Pail.create(local, path);
        Multimap<String, String> expected = HashMultimap.create();

        List<String> builder = new ArrayList<String>();
        for(int i=0; i < Math.random()*1000; i++) {
            String val = "a" + i;
            builder.add(val);
            expected.put("", val);
        }
        emitToPail(pail, "a", builder);

        builder = new ArrayList<String>();
        for(int i=0; i < Math.random()*1000000; i++) {
            String val = "b" + i;
            builder.add(val);
            expected.put("a/b/c/ddd", val);
        }
        emitToPail(pail, "a/b/c/ddd/1", builder);


        builder = new ArrayList<String>();
        for(int i=0; i < Math.random()*1000000; i++) {
            String val = "c" + i;
            builder.add(val);
            expected.put("a/b/d", val);
        }
        emitToPail(pail, "a/b/d/111", builder);

        Multimap<String, String> results = HashMultimap.create();


        CombineFileInputFormat informat = (CombineFileInputFormat) format.getInputFormatClass().newInstance();
        JobConf conf = new JobConf();
        FileInputFormat.addInputPath(conf, new Path(path));
        InputSplit[] splits = informat.getSplits(conf, 10000);
        assertTrue(splits.length == 1); //want only one split for all three files
        for(InputSplit split: splits) {
            RecordReader<Text, BytesWritable> rr = informat.getRecordReader(split, conf, Reporter.NULL);
            Text t = new Text();
            BytesWritable b = new BytesWritable();
            while(rr.next(t, b)) {
                results.put(t.toString(), new String(Utils.getBytes(b)));
            }
            rr.close();
        }
        assertEquals(expected, results);

        //TODO: test reading from a subbucket

    }


    protected abstract PailSpec getSpec();
}
