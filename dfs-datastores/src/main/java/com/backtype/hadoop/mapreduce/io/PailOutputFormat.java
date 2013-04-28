package com.backtype.hadoop.mapreduce.io;

import com.backtype.hadoop.formats.RecordOutputStream;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.backtype.support.Utils.getObject;

public class PailOutputFormat extends FileOutputFormat<Text, BytesWritable> {
    public static Logger LOG = LoggerFactory.getLogger(PailOutputFormat.class);
    public static final String SPEC_ARG = "pail_spec_arg";

    // we limit the size of outputted files because of s3 file limits
    public static final long FILE_LIMIT_SIZE_BYTES = 1L * 1024 * 1024 * 1024; // 1GB


    /**
     * Change this to just use Pail#writeObject - automatically fix up BytesWritable
     */
    public static class PailRecordWriter extends RecordWriter<Text, BytesWritable> {
        private Pail _pail;
        private String _unique;

        protected static class OpenAttributeFile {
            public String attr;
            public String filename;
            public RecordOutputStream os;
            public long numBytesWritten = 0;

            public OpenAttributeFile(String attr, String filename, RecordOutputStream os) {
                this.attr = attr;
                this.filename = filename;
                this.os = os;
            }
        }

        private Map<String, OpenAttributeFile> _outputters = new HashMap<String, OpenAttributeFile>();
        private int writtenRecords = 0;
        private int numFilesOpened = 0;

        public PailRecordWriter(TaskAttemptContext context, PailOutputFormat pailOutputFormat) throws IOException {
            PailSpec spec = (PailSpec) getObject(context.getConfiguration(), SPEC_ARG);

            Path path = new Path(context.getConfiguration().get("mapred.output.dir"));
            FileSystem fs = path.getFileSystem(context.getConfiguration());

            Pail.create(fs, path.toString(), spec, false);
            // TODO:
            // We are using a UUID here, since if we read from the Pail created by MR Output
            // the files were just getting over-written. Not sure if the desired behaviour
            _unique = getUniqueFile(context, UUID.randomUUID().toString(), "");
            _pail = Pail.create(fs, pailOutputFormat.getDefaultWorkFile(context, null).getParent().toString(), spec, false);
        }

        @Override
        public void write(Text k, BytesWritable v) throws IOException {
            String attr = k.toString();
            OpenAttributeFile oaf = _outputters.get(attr);
            if (oaf != null && oaf.numBytesWritten >= FILE_LIMIT_SIZE_BYTES) {
                closeAttributeFile(oaf);
                oaf = null;
                _outputters.remove(attr);
            }
            if (oaf == null) {
                String filename;
                if (!attr.isEmpty()) {
                    filename = attr + "/" + _unique + numFilesOpened;
                } else {
                    filename = _unique + numFilesOpened;
                }
                numFilesOpened++;
                LOG.info("Opening " + filename + " for attribute " + attr);
                //need overwrite for situations where regular FileOutputCommitter isn't used (like S3)
                oaf = new OpenAttributeFile(attr, filename, _pail.openWrite(filename, true));
                _outputters.put(attr, oaf);
            }
            oaf.os.writeRaw(v.getBytes(), 0, v.getLength());
            oaf.numBytesWritten += v.getLength();
            logProgress();
        }

        protected void logProgress() {
            writtenRecords++;
            if (writtenRecords % 100000 == 0) {
                for (OpenAttributeFile oaf : _outputters.values()) {
                    LOG.info("Attr:" + oaf.attr + " Filename:" + oaf.filename + " Bytes written:" + oaf.numBytesWritten);
                }
            }
        }

        protected void closeAttributeFile(OpenAttributeFile oaf) throws IOException {
            LOG.info("Closing " + oaf.filename + " for attr " + oaf.attr);
            //print out the size of the file here
            oaf.os.close();
            LOG.info("Closed " + oaf.filename + " for attr " + oaf.attr);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            for (String key : _outputters.keySet()) {
                closeAttributeFile(_outputters.get(key));
                context.progress();
            }
            _outputters.clear();
        }
    }

    @Override
    public RecordWriter<Text, BytesWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new PailRecordWriter(context, this);
    }

    @Override
    public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
//        There is no way in the new API to check for FileOutputCommitter without TaskAttemptContext,
//        since we are extending the FileOutputFormat the default OutputCommitter will be FileOutputCommitter

//        because this outputs multiple files, doesn't work with speculative execution on something like EMR with S3
//        if(!(conf.getOutputCommitter() instanceof FileOutputCommitter)) {
//            if(conf.getMapSpeculativeExecution() && conf.getNumReduceTasks()==0 || conf.getReduceSpeculativeExecution()) {
//                throw new IllegalArgumentException("Cannot use speculative execution with PailOutputFormat unless FileOutputCommitter is enabled");
//            }
//        }
    }


}
