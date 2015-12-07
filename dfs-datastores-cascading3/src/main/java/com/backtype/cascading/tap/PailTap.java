package com.backtype.cascading.tap;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;

import com.backtype.hadoop.pail.BinaryPailStructure;
import com.backtype.hadoop.pail.DefaultPailStructure;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailFormatFactory;
import com.backtype.hadoop.pail.PailOutputFormat;
import com.backtype.hadoop.pail.PailPathLister;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;
import com.backtype.support.Utils;
import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.hadoop.TupleSerialization;

public class PailTap extends Hfs {
  private static Logger LOG = Logger.getLogger(PailTap.class);

  public static PailSpec makeSpec(PailSpec given, PailStructure structure) {
    return (given == null) ? PailFormatFactory.getDefaultCopy().setStructure(structure) :
        given.setStructure(structure);
  }

  public static class PailTapOptions implements Serializable {
    public PailSpec spec = null;
    public String fieldName = "bytes";
    public List<String>[] attrs = null;
    public PailPathLister lister = null;

    public PailTapOptions() {

    }

    public PailTapOptions(PailSpec spec, String fieldName, List<String>[] attrs,
        PailPathLister lister) {
      this.spec = spec;
      this.fieldName = fieldName;
      this.attrs = attrs;
      this.lister = lister;
    }
  }

  public class PailScheme
      extends Scheme<Configuration, RecordReader, OutputCollector, Object[], Object[]> {
    private PailTapOptions _options;

    public PailScheme(PailTapOptions options) {
      super(new Fields("pail_root", options.fieldName), Fields.ALL);
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
    public void sourceConfInit(FlowProcess<? extends Configuration> process,
        Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf) {
      Pail p;
      try {
        p = new Pail(_pailRoot, conf); //make sure it exists
      } catch (IOException e) {
        throw new TapException(e);
      }
      
      JobConf jobConf = new JobConf(conf);
      
      jobConf.setInputFormat(p.getFormat().getInputFormatClass());
      PailFormatFactory.setPailPathLister(jobConf, _options.lister);
    }

    @Override public void sinkConfInit(FlowProcess<? extends Configuration> flowProcess,
        Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf) {

      JobConf jobConf = new JobConf(conf);

      jobConf.setOutputFormat(PailOutputFormat.class);
      Utils.setObject(jobConf, PailOutputFormat.SPEC_ARG, getSpec());
      try {
        Pail.create(getFileSystem(jobConf), _pailRoot, getSpec(), true);
      } catch (IOException e) {
        throw new TapException(e);
      }
    }

    @Override
    public void sourcePrepare(FlowProcess<? extends Configuration> flowProcess,
        SourceCall<Object[], RecordReader> sourceCall) {
      sourceCall.setContext(new Object[2]);

      sourceCall.getContext()[0] = sourceCall.getInput().createKey();
      sourceCall.getContext()[1] = sourceCall.getInput().createValue();
    }

    @Override
    public boolean source(FlowProcess<? extends Configuration> process,
        SourceCall<Object[], RecordReader> sourceCall) throws IOException {
      Object k = sourceCall.getContext()[0];
      Object v = sourceCall.getContext()[1];
      boolean result = sourceCall.getInput().next(k, v);
      if (!result) { return false; }
      String relPath = ((Text) k).toString();
      Object value = deserialize((BytesWritable) v);
      sourceCall.getIncomingEntry().setTuple(new Tuple(relPath, value));
      return true;
    }

    @Override
    public void sink(FlowProcess<? extends Configuration> process, SinkCall<Object[], OutputCollector> sinkCall)
        throws IOException {
      TupleEntry tuple = sinkCall.getOutgoingEntry();

      Object obj = tuple.getObject(0);
      String key;
      //a hack since byte[] isn't natively handled by hadoop
      if (getStructure() instanceof DefaultPailStructure) {
        key = getCategory(obj);
      } else {
        key = Utils.join(getStructure().getTarget(obj), "/") + getCategory(obj);
      }
      if (bw == null) { bw = new BytesWritable(); }
      if (keyW == null) { keyW = new Text(); }
      serialize(obj, bw);
      keyW.set(key);
      sinkCall.getOutput().collect(keyW, bw);
    }

  }

  private String _pailRoot;
  private PailTapOptions _options;

  protected String getCategory(Object obj) {
    return "";
  }

  public PailTap(String root, PailTapOptions options) {
    _options = options;
    setStringPath(root);
    setScheme(new PailScheme(options));
    _pailRoot = root;
  }

  public PailTap(String root) {
    this(root, new PailTapOptions());
  }

  @Override
  public String getIdentifier() {
      if (_options.attrs != null && _options.attrs.length > 0) {
          String rel = "";
          for (List<String> attr : _options.attrs) {
              rel += Utils.join(attr, Path.SEPARATOR);
          }
          return getPath().toString() + Path.SEPARATOR + rel;
      } else {
          return getPath().toString();
      }
  }

  @Override
  public long getModifiedTime(Configuration conf) throws IOException {
    // TODO: We should either set sinkMode.REPLACE or return something real here when used as a source.
    return 0;
  }

  @Override
  public boolean deleteResource(Configuration conf) throws IOException {
    throw new UnsupportedOperationException();
  }


  //no good way to override this, just had to copy/paste and modify
  @Override
  public void sourceConfInit(FlowProcess<? extends Configuration> process, Configuration conf) {
    try {
      JobConf jobConf = new JobConf(conf);
    
      Path root = getQualifiedPath(jobConf);
      if (_options.attrs != null && _options.attrs.length > 0) {
        Pail pail = new Pail(_pailRoot, jobConf);
        for (List<String> attr : _options.attrs) {
          String rel = Utils.join(attr, "/");
          pail.getSubPail(rel); //ensure the path exists
          Path toAdd = new Path(root, rel);
          LOG.info("Adding input path " + toAdd.toString());
          FileInputFormat.addInputPath(jobConf, toAdd);
        }
      } else {
        FileInputFormat.addInputPath(jobConf, root);
      }

      getScheme().sourceConfInit(process, this, jobConf);
      makeLocal(jobConf, getQualifiedPath(jobConf), "forcing job to local mode, via source: ");
      TupleSerialization.setSerializations(jobConf);
    } catch (IOException e) {
      throw new TapException(e);
    }
  }

  private void makeLocal(Configuration conf, Path qualifiedPath, String infoMessage) {
    if (!conf.get("mapred.job.tracker", "").equalsIgnoreCase("local") && qualifiedPath.toUri()
        .getScheme().equalsIgnoreCase("file")) {
      if (LOG.isInfoEnabled()) { LOG.info(infoMessage + toString()); }

      conf.set("mapred.job.tracker", "local"); // force job to run locally
    }
  }

  @Override
  public void sinkConfInit(FlowProcess<? extends Configuration> process, Configuration conf) {
    if (_options.attrs != null && _options.attrs.length > 0) {
      throw new TapException("can't declare attributes in a sink");
    }

    super.sinkConfInit(process, conf);
  }

  @Override
  public boolean commitResource(Configuration conf) throws IOException {
    Pail p = Pail.create(_pailRoot, ((PailScheme) getScheme()).getSpec(), false);
    FileSystem fs = p.getFileSystem();
    Path tmpPath = new Path(_pailRoot, "_temporary");
    if (fs.exists(tmpPath)) {
      LOG.info("Deleting _temporary directory left by Hadoop job: " + tmpPath.toString());
      fs.delete(tmpPath, true);
    }

    Path tmp2Path = new Path(_pailRoot, "_temporary2");
    if (fs.exists(tmp2Path)) {
      LOG.info("Deleting _temporary2 directory: " + tmp2Path.toString());
      fs.delete(tmp2Path, true);
    }

    Path logPath = new Path(_pailRoot, "_logs");
    if (fs.exists(logPath)) {
      LOG.info("Deleting _logs directory left by Hadoop job: " + logPath.toString());
      fs.delete(logPath, true);
    }

    return true;
  }

  @Override
  public int hashCode() {
    return _pailRoot.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    if (!getClass().equals(object.getClass())) {
      return false;
    }
    PailTap other = (PailTap) object;
    Set<List<String>> myattrs = new HashSet<List<String>>();
    if (_options.attrs != null) {
      Collections.addAll(myattrs, _options.attrs);
    }
    Set<List<String>> otherattrs = new HashSet<List<String>>();
    if (other._options.attrs != null) {
      Collections.addAll(otherattrs, other._options.attrs);
    }
    return _pailRoot.equals(other._pailRoot) && myattrs.equals(otherattrs);
  }

  private Path getQualifiedPath(JobConf conf) throws IOException {
    return getPath().makeQualified(getFileSystem(conf));
  }
}
