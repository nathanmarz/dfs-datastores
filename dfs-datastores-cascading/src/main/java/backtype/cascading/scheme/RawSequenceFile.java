package backtype.cascading.scheme;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class RawSequenceFile
    extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
  public RawSequenceFile(String keyField, String valueField) {
    super(new Fields(keyField, valueField));
  }

  public void sourceConfInit(HadoopFlowProcess process, Tap tap, JobConf conf) {
    conf.setInputFormat(SequenceFileInputFormat.class);
  }

  @Override public void sourceConfInit(FlowProcess<JobConf> jobConfFlowProcess,
      Tap<JobConf, RecordReader, OutputCollector> jobConfRecordReaderOutputCollectorTap,
      JobConf entries) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> prcs,
      Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    throw new TapException("cannot use as a sink");
  }

  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) {
    sourceCall.setContext(new Object[2]);

    sourceCall.getContext()[0] = sourceCall.getInput().createKey();
    sourceCall.getContext()[1] = sourceCall.getInput().createValue();
  }

  @Override
  public boolean source(FlowProcess<JobConf> prcs, SourceCall<Object[], RecordReader> sourceCall)
      throws IOException {

    Object key = sourceCall.getContext()[0];
    Object value = sourceCall.getContext()[1];

    boolean result = sourceCall.getInput().next(key, value);

    if (!result) {
      return false;
    }

    sourceCall.getIncomingEntry().setTuple(new Tuple(key, value));
    return true;
  }

  @Override
  public void sink(FlowProcess<JobConf> prcs, SinkCall<Object[], OutputCollector> sc)
      throws IOException {
    throw new TapException("cannot use as a sink");
  }
}
