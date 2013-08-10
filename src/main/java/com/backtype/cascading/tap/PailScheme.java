/**
 * 
 */
package com.backtype.cascading.tap;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.backtype.hadoop.pail.BinaryPailStructure;
import com.backtype.hadoop.pail.DefaultPailStructure;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailFormatFactory;
import com.backtype.hadoop.pail.PailOutputFormat;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;
import com.backtype.support.Utils;

public class PailScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
    /**
     * 
     */
    private final PailTap mPailTap;
    private PailTapOptions _options;

    public PailScheme(PailTap pailTap, PailTapOptions options) {
        super(new Fields("pail_root").append(options.outputFields), Fields.ALL);
        mPailTap = pailTap;
        _options = options;
    }

    public PailSpec getSpec() {
        return _options.spec;
    }

    private transient BytesWritable bw;
    private transient Text keyW;

    protected Object deserialize(BytesWritable record) {
        PailStructure structure = getStructure();
        if (structure instanceof BinaryPailStructure) {
            return record;
        } else {
            return structure.deserialize(Utils.getBytes(record));
        }
    }

    protected void serialize(Object obj, BytesWritable ret) {
        if (obj instanceof BytesWritable) {
            ret.set((BytesWritable) obj);
        } else {
            byte[] b = getStructure().serialize(obj);
            ret.set(b, 0, b.length);
        }
    }

    private transient PailStructure _structure;

    public PailStructure getStructure() {
        if (_structure == null) {
            if (getSpec() == null) {
                _structure = PailFormatFactory.getDefaultCopy().getStructure();
            } else {
                _structure = getSpec().getStructure();
            }
        }
        return _structure;
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> process, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        Pail p;
        try {
            p = new Pail(mPailTap.getPailRoot()); // make sure it exists
        } catch (IOException e) {
            throw new TapException(e);
        }
        conf.setInputFormat(p.getFormat().getInputFormatClass());
        PailFormatFactory.setPailPathLister(conf, _options.lister);
    }

    public FileSystem getFileSystem(JobConf conf) {
        URI scheme = mPailTap.getURIScheme(conf);
        try
        {
            return FileSystem.get(scheme, conf);
        } catch (IOException exception)
        {
            throw new TapException("unable to get handle to get filesystem for: " + scheme.getScheme(), exception);
        }
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess,
            Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        conf.setOutputFormat(PailOutputFormat.class);
        Utils.setObject(conf, PailOutputFormat.SPEC_ARG, getSpec());
        try {
            Pail.create(getFileSystem(conf), mPailTap._pailRoot, getSpec(), true);
        } catch (IOException e) {
            throw new TapException(e);
        }
    }

    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) {
        sourceCall.setContext(new Object[2]);
        sourceCall.getContext()[0] = sourceCall.getInput().createKey();
        sourceCall.getContext()[1] = sourceCall.getInput().createValue();
    }

    @Override
    public boolean source(FlowProcess<JobConf> process,
            SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        Object k = sourceCall.getContext()[0];
        Object v = sourceCall.getContext()[1];
        boolean result = sourceCall.getInput().next(k, v);
        if (!result) {
            return false;
        }
        String relPath = ((Text) k).toString();

        Object value = deserialize((BytesWritable) v);
        if (_options.useTuple  && value instanceof Tuple) {
            sourceCall.getIncomingEntry().setTuple(new Tuple(relPath).append((Tuple) value));
        } else {
            sourceCall.getIncomingEntry().setTuple(new Tuple(relPath, value));

        }
        return true;
    }

    @Override
    public void sink(FlowProcess<JobConf> process, SinkCall<Object[], OutputCollector> sinkCall)
            throws IOException {
        TupleEntry tupleEntry = sinkCall.getOutgoingEntry();

        Tuple selected = tupleEntry.selectTuple(_options.outputFields);

        Object obj = (_options.useTuple) ? selected : selected.getObject(0);

        String key;
        // a hack since byte[] isn't natively handled by hadoop
        if (getStructure() instanceof DefaultPailStructure) {
            key = mPailTap.getCategory(obj);
        } else {
            key = Utils.join(getStructure().getTarget(obj), "/") + mPailTap.getCategory(obj);
        }
        if (bw == null) {
            bw = new BytesWritable();
        }
        if (keyW == null) {
            keyW = new Text();
        }

        serialize(obj, bw);
        keyW.set(key);
        sinkCall.getOutput().collect(keyW, bw);
    }

}