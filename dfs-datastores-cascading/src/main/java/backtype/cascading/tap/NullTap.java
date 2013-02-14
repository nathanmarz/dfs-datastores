package backtype.cascading.tap;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class NullTap extends Tap {

  public static class NullScheme
      extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

    public NullScheme() {
      super(Fields.ALL);
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> prcs,
        Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
      throw new IllegalArgumentException("Cannot use as a source");
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> prcs,
        Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
      conf.setOutputFormat(NullOutputFormat.class);
    }

    @Override
    public boolean source(FlowProcess<JobConf> prcs, SourceCall<Object[], RecordReader> sc)
        throws IOException {
      throw new IllegalArgumentException("cannot source");
    }

    @Override
    public void sink(FlowProcess<JobConf> prcs, SinkCall<Object[], OutputCollector> sourceCall)
        throws IOException {
    }
  }

  public static class NullTupleEntryCollector extends TupleEntryCollector {
    @Override protected void collect(TupleEntry tupleEntry) throws IOException { }
  }


  public NullTap() {
    super(new NullScheme());
  }

  @Override public String getIdentifier() {
    return "/dev/null";
  }

  @Override public TupleEntryIterator openForRead(FlowProcess flowProcess, Object o)
      throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override public TupleEntryCollector openForWrite(FlowProcess flowProcess, Object o)
      throws IOException {
    return new NullTupleEntryCollector();
  }

  @Override public boolean createResource(Object o) throws IOException {
    return true;
  }

  @Override public boolean deleteResource(Object o) throws IOException {
    return true;
  }

  @Override public boolean resourceExists(Object o) throws IOException {
    return false;
  }

  @Override public long getModifiedTime(Object config) throws IOException {
    return System.currentTimeMillis();
  }

  @Override
  public boolean equals(Object object) {
    return object.getClass() == this.getClass() && this == object;
  }
}
