package com.backtype.cascading.tap;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import cascading.operation.OperationCall;
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

import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.io.HadoopTupleInputStream;
import cascading.tuple.hadoop.io.HadoopTupleOutputStream;
import cascading.tuple.io.TupleInputStream;
import cascading.tuple.io.TupleOutputStream;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

class TupleSerializationUtil implements Serializable {
  private static final int BUFFER_SIZE = 4096;
  private final JobConf jobConf;
  private transient TupleSerialization serialization = null;
  private transient ByteArrayOutputStream bytesOutputStream = null;
  private transient TupleOutputStream tupleOutputStream = null;
  private transient Serializer<Tuple> tupleSerializer = null;
  private transient Deserializer<Tuple> tupleDeserializer = null;

  public TupleSerializationUtil(JobConf jobConf) {
    this.jobConf = jobConf;
  }

  public byte[] serialize(Tuple tuple) throws IOException {
    initSerializer();
    bytesOutputStream.reset();
    tupleSerializer.open(tupleOutputStream);
    tupleSerializer.serialize(tuple);
    return bytesOutputStream.toByteArray();
  }

  public Tuple deserialize(byte[] bytes) throws IOException {
    initDeserializer();
    Tuple tuple = new Tuple();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    TupleInputStream tupleInputStream = new HadoopTupleInputStream(inputStream, serialization.getElementReader());
    tupleDeserializer.open(tupleInputStream);
    tupleDeserializer.deserialize(tuple);
    return tuple;
  }

  private void initSerializer() {
    init();
    if (bytesOutputStream == null) {
      bytesOutputStream = new ByteArrayOutputStream(BUFFER_SIZE);
    }
    if (tupleOutputStream == null) {
      tupleOutputStream = new HadoopTupleOutputStream(bytesOutputStream, serialization.getElementWriter());
    }
    if (tupleSerializer == null) {
      tupleSerializer = serialization.getSerializer(Tuple.class);
    }
  }

  private void initDeserializer() {
    init();
    if (tupleDeserializer == null) {
      tupleDeserializer = serialization.getDeserializer(Tuple.class);
    }
  }

  private void init() {
    if (serialization == null) {
      serialization = new TupleSerialization(jobConf);
    }
  }
}



public class PailTap extends Hfs {
  private static Logger LOG = Logger.getLogger(PailTap.class);

  public static PailSpec makeSpec(PailSpec given, PailStructure structure) {
    return (given == null) ? PailFormatFactory.getDefaultCopy().setStructure(structure) :
        given.setStructure(structure);
  }

  public static class PailTapOptions implements Serializable {
    public PailSpec getSpec() {
		return spec;
	}

	public String getFieldName() {
		return fieldName;
	}

	public Fields getOutputFields() {
		return outputFields;
	}

	public List<String>[] getAttrs() {
		return attrs;
	}

	public PailPathLister getLister() {
		return lister;
	}

	private PailSpec spec = null;
	private String fieldName = "bytes";
	private Fields outputFields = new Fields( fieldName );
	private boolean useHadoopSerialization = false;
	private List<String>[] attrs = null;
	private PailPathLister lister = null;
	public boolean wrapInTuple = false;

    public PailTapOptions() {

    }
    
    public PailTapOptions spec( PailSpec spec ) {
        this.spec = spec;
        return this;
    }
    
    public PailTapOptions fieldName( String fieldName ) {
        this.fieldName = fieldName;
        return this;
    }
    
    public PailTapOptions wrapInTuple( boolean wrapInTuple ) {
        this.wrapInTuple = wrapInTuple;
        return this;
    }
    
    public PailTapOptions useHadoopSerialization( boolean useHadoopSerialization ) {
        this.useHadoopSerialization = useHadoopSerialization;
        return this;
    }
    
    public PailTapOptions outputFields( Fields outputFields ) {
        this.outputFields = outputFields;
        return this;
    }
    
    public PailTapOptions attrs( List<String>[] attrs ) {
        this.attrs = attrs;
        return this;
    }
    
    public PailTapOptions lister( PailPathLister lister ) {
        this.lister = lister;
        return this;
    }
    
  }

  public class PailScheme
      extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
    private PailTapOptions _options;

    public PailScheme(PailTapOptions options) {
      super(new Fields("pail_root").append(options.outputFields), Fields.ALL);
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
    private transient TupleSerializationUtil tupleSerializationUtil;

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
    public void sourceConfInit(FlowProcess<JobConf> process,
        Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
      Pail p;
      try {
        p = new Pail(_pailRoot); //make sure it exists
      } catch (IOException e) {
        throw new TapException(e);
      }
      conf.setInputFormat(p.getFormat().getInputFormatClass());
      PailFormatFactory.setPailPathLister(conf, _options.lister);
      
      tupleSerializationUtil =  new TupleSerializationUtil(conf);

    }

    @Override public void sinkConfInit(FlowProcess<JobConf> flowProcess,
        Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
      conf.setOutputFormat(PailOutputFormat.class);
      Utils.setObject(conf, PailOutputFormat.SPEC_ARG, getSpec());
      try {
        Pail.create(getFileSystem(conf), _pailRoot, getSpec(), true);
      } catch (IOException e) {
        throw new TapException(e);
      }
    }

    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess,
        SourceCall<Object[], RecordReader> sourceCall) {
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
      if (!result) { return false; }
      String relPath = ((Text) k).toString();
       
      Object value = _options.useHadoopSerialization ? tupleSerializationUtil.deserialize(Utils.getBytes((BytesWritable) v)) : deserialize((BytesWritable) v);
      if( _options.wrapInTuple ) {
          sourceCall.getIncomingEntry().setTuple(new Tuple(relPath).append((Tuple)value));
      } else {
          sourceCall.getIncomingEntry().setTuple(new Tuple(relPath, value));

      }
      return true;
    }

    @Override
    public void sink(FlowProcess<JobConf> process, SinkCall<Object[], OutputCollector> sinkCall)
        throws IOException {
      TupleEntry tuple = sinkCall.getOutgoingEntry();

      Tuple selected = tuple.selectTuple(_options.outputFields);
      
      Object obj = _options.wrapInTuple ? selected : selected.getObject(0);

      String key;
      //a hack since byte[] isn't natively handled by hadoop
      if (getStructure() instanceof DefaultPailStructure) {
        key = getCategory(obj);
      } else {
        key = Utils.join(getStructure().getTarget(obj), "/") + getCategory(obj);
      }
      if (bw == null) { bw = new BytesWritable(); }
      if (keyW == null) { keyW = new Text(); }
      
      if( _options.useHadoopSerialization ) {
          byte[] bytes = tupleSerializationUtil.serialize(selected);
          bw.set(bytes, 0, bytes.length);
      } else {
          serialize(obj, bw);
      }
      keyW.set(key);
      sinkCall.getOutput().collect(keyW, bw);
    }

  }

  protected String _pailRoot;
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
  public boolean deleteResource(JobConf conf) throws IOException {
    throw new UnsupportedOperationException();
  }


  //no good way to override this, just had to copy/paste and modify
  @Override
  public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf) {
    try {
      Path root = getQualifiedPath(conf);
      if (_options.attrs != null && _options.attrs.length > 0) {
        Pail pail = new Pail(_pailRoot);
        for (List<String> attr : _options.attrs) {
          String rel = Utils.join(attr, "/");
          pail.getSubPail(rel); //ensure the path exists
          Path toAdd = new Path(root, rel);
          LOG.info("Adding input path " + toAdd.toString());
          FileInputFormat.addInputPath(conf, toAdd);
        }
      } else {
        FileInputFormat.addInputPath(conf, root);
      }

      getScheme().sourceConfInit(process, this, conf);
      makeLocal(conf, getQualifiedPath(conf), "forcing job to local mode, via source: ");
      TupleSerialization.setSerializations(conf);
    } catch (IOException e) {
      throw new TapException(e);
    }
  }

  public List<Path> getPaths() {
      final List<Path> paths = new ArrayList<Path>();
      if (_options.attrs != null && _options.attrs.length > 0) {
          for (List<String> attr : _options.attrs) {
              String rel = Utils.join(attr, "/");
              paths.add(new Path(_pailRoot, rel));
          }
      } else {
          paths.add(new Path(_pailRoot));
      }
      return paths;
  }
  
  private void makeLocal(JobConf conf, Path qualifiedPath, String infoMessage) {
    if (!conf.get("mapred.job.tracker", "").equalsIgnoreCase("local") && qualifiedPath.toUri()
        .getScheme().equalsIgnoreCase("file")) {
      if (LOG.isInfoEnabled()) { LOG.info(infoMessage + toString()); }

      conf.set("mapred.job.tracker", "local"); // force job to run locally
    }
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
    if (_options.attrs != null && _options.attrs.length > 0) {
      throw new TapException("can't declare attributes in a sink");
    }

    super.sinkConfInit(process, conf);
  }

  @Override
  public boolean commitResource(JobConf conf) throws IOException {
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
