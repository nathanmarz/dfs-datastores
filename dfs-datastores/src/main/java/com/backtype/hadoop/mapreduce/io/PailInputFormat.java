package com.backtype.hadoop.mapreduce.io;

import com.backtype.hadoop.pail.Pail;
import com.backtype.support.Utils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.backtype.hadoop.pail.PailFormatFactory.getPailPaths;


public class PailInputFormat extends SequenceFileInputFormat<PailRecordInfo, BytesWritable> {
    private Pail _currPail;

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> ret = new ArrayList<InputSplit>();
        Path[] roots = FileInputFormat.getInputPaths(job);
        for (Path root : roots) {
            _currPail = new Pail(root.toString());
            List<InputSplit> splits = super.getSplits(job);
            for (InputSplit split : splits) {
                ret.add(new PailInputSplit(_currPail.getFileSystem(), _currPail.getInstanceRoot(), _currPail.getSpec(), (FileSplit) split));
            }
        }
        return ret;
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<Path> paths = getPailPaths(_currPail, job.getConfiguration());
        FileSystem fs = _currPail.getFileSystem();
        List<FileStatus> ret = new ArrayList<FileStatus>();
        for (Path path : paths) {
            ret.add(fs.getFileStatus(path.makeQualified(fs)));
        }
        return ret;

    }


    @Override
    public RecordReader<PailRecordInfo, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new SequenceFilePailRecordReader((PailInputSplit) split, context);
    }

    public static class SequenceFilePailRecordReader extends RecordReader<PailRecordInfo, BytesWritable> {
        private static Logger LOG = LoggerFactory.getLogger(SequenceFilePailRecordReader.class);
        public static final int NUM_TRIES = 10;

        PailInputSplit split;
        int recordsRead;
        TaskAttemptContext context;

        SequenceFileRecordReader<BytesWritable, NullWritable> delegate;


        public SequenceFilePailRecordReader(PailInputSplit split, TaskAttemptContext context) throws IOException {
            this.split = split;
            this.recordsRead = 0;
            this.context = context;
            LOG.info("Processing pail file " + split.getPath().toString());
        }

        private void resetDelegate(TaskAttemptContext context) throws IOException, InterruptedException {
            this.delegate = new SequenceFileRecordReader<BytesWritable, NullWritable>();
            delegate.initialize(split, context);
            for(int i=0; i<recordsRead; i++) {
                delegate.nextKeyValue();
            }
        }

        private void progress() {
            if(context !=null) {
                context.progress();
            }
        }

        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            resetDelegate(context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {

            /**
             * There's 2 bugs that happen here, both resulting in indistinguishable EOFExceptions.
             *
             * 1. Random EOFExceptions when reading data off of S3. Usually succeeds on the 2nd try.
             * 2. Corrupted files most likely due to network corruption (which isn't handled by Hadoop/S3 integration).
             *    These always result in error.
             *
             * The strategy is to retry a few times. If it fails every time then we're in case #2, and the best thing we can do
             * is continue on and accept the data loss. If we're in case #1, it'll just succeed.
             */
            for(int i=0; i<NUM_TRIES; i++) {
                try {
                    boolean ret = delegate.nextKeyValue();
                    recordsRead++;
                    return ret;
                } catch(EOFException e) {
                    progress();
                    Utils.sleep(10000); //in case it takes time for S3 to recover
                    progress();
                    //this happens due to some sort of S3 corruption bug.
                    LOG.error("Hit an EOF exception while processing file " + split.getPath().toString() +
                            " with records read = " + recordsRead);
                    resetDelegate(context);
                }
            }
            //stop trying to read the file at this point and discard the rest of the file
            return false;

        }

        @Override
        public PailRecordInfo getCurrentKey() throws IOException, InterruptedException {
            return new PailRecordInfo(split.getPath().toString(), split.getPailRelPath(), split.getStart(), recordsRead);
        }

        @Override
        public BytesWritable getCurrentValue() throws IOException, InterruptedException {
            return delegate.getCurrentKey();
        }

        @Override
        public float getProgress() throws IOException {
            return delegate.getProgress();
        }

    }

}
