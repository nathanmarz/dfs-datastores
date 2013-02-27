package com.backtype.cascading.tap;

import cascading.flow.hadoop.HadoopFlowConnector;
import com.backtype.cascading.tap.PailTap.PailTapOptions;
import com.backtype.hadoop.pail.Pail;
import com.backtype.support.FSTestCase;
import com.backtype.support.Utils;
import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.operation.*;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.backtype.support.CascadingUtils.identityFlow;
import static com.backtype.support.TestUtils.*;


public class PailTapTest extends FSTestCase {

    protected static class Add1 extends BaseOperation implements Function {
        public Add1() {
            super(new Fields("result"));
        }

        public void operate(FlowProcess fp, FunctionCall fc) {
            TupleEntry t = fc.getArguments();
            String res = new String(Utils.getBytes((BytesWritable) t.get(0))) + "1";
            fc.getOutputCollector().add(new Tuple(new BytesWritable(res.getBytes())));
        }

    }

    public void testSimpleFlow() throws Exception {
        String pail = getTmpPath(fs, "pail");
        String sinkpath = getTmpPath(fs, "sink");
        emitToPail(Pail.create(fs, pail), "aaaa", "a", "b", "c", "d", "e");

        Tap source = new PailTap(pail);
        Pipe pipe = new Pipe("pipe");
        pipe = new Each(pipe, new Fields("bytes"), new Add1(), new Fields("result"));
        PailTapOptions options = new PailTapOptions().fieldName("result");
        Tap sink = new PailTap(sinkpath, options);

        new HadoopFlowConnector().connect(source, sink, pipe).complete();

        Set<String> records = new HashSet<String>(getPailRecords(new Pail(fs, sinkpath)));
        assertTrue(records.contains("a1"));
        assertTrue(records.contains("b1"));
        assertTrue(records.contains("c1"));
        assertTrue(records.contains("d1"));
        assertTrue(records.contains("e1"));
        assertEquals(5, records.size());
    }

    public static class MkdirsFilter extends BaseOperation implements Filter {
        private String path;

        public MkdirsFilter(String path) {
            super();
            this.path = path;
        }

        public boolean isRemove(FlowProcess fp, FilterCall fc) {
            try {
                FileSystem.get(new Configuration()).mkdirs(new Path(path));
                return false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public void testGarbagePathCleanup() throws Exception {
        String sourcePath = getTmpPath(fs, "source");
        String sinkPath = getTmpPath(fs, "sink");
        emitToPail(Pail.create(fs, sourcePath), "a", "b", "c");
        fs.mkdirs(new Path(sourcePath, "_temporary"));

        Pipe pipe = new Pipe("pipe");
        pipe = new Each(pipe, new Fields("bytes"), new Identity());
        pipe = new Each(pipe, new Fields("bytes"), new MkdirsFilter(sinkPath + "/_temporary2"));

        PailTap source = new PailTap(sourcePath);
        PailTap sink = new PailTap(sinkPath);
        new HadoopFlowConnector().connect(source, sink, pipe).complete();

        assertTrue(fs.exists(new Path(sourcePath, "_temporary")));
        assertFalse(fs.exists(new Path(sinkPath, "_temporary2")));
    }

    public void testWriteToExistingPail() throws Exception {
        String sourcePath = getTmpPath(fs, "source");
        String sinkPath = getTmpPath(fs, "sink");
        emitToPail(Pail.create(fs, sourcePath), "a", "b", "c");
        Pail.create(fs, sinkPath);

        try {
            identityFlow(new PailTap(sourcePath), new PailTap(sinkPath), new Fields("bytes"));
            fail("should fail!");
        } catch (FlowException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }
}
