package backtype.hadoop;

import backtype.hadoop.formats.RecordInputStream;
import backtype.hadoop.formats.RecordOutputStream;
import backtype.hadoop.formats.RecordStreamFactory;
import backtype.support.SubsetSum;
import backtype.support.SubsetSum.Value;
import backtype.support.Utils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;


public class Consolidator {
    public static final long DEFAULT_CONSOLIDATION_SIZE = 1024*1024*127; //127 MB
    private static final String ARGS = "consolidator_args";

    private static Thread shutdownHook;
    private static RunningJob job = null;

    public static class ConsolidatorArgs implements Serializable {
        public RecordStreamFactory streams;
        public PathLister pathLister;
        public List<String> dirs;
        public long targetSizeBytes;
        public String extension;


        public ConsolidatorArgs(RecordStreamFactory streams, PathLister pathLister, List<String> dirs,
                long targetSizeBytes, String extension) {
            this.streams = streams;
            this.pathLister = pathLister;
            this.dirs = dirs;
            this.targetSizeBytes = targetSizeBytes;
            this.extension = extension;
        }
    }

    public static void consolidate(FileSystem fs, String targetDir, RecordStreamFactory streams,
            PathLister pathLister, long targetSizeBytes) throws IOException {
        consolidate(fs, targetDir, streams, pathLister, targetSizeBytes, "");
    }

    public static void consolidate(FileSystem fs, String targetDir, RecordStreamFactory streams,
            PathLister pathLister, String extension) throws IOException {
        consolidate(fs, targetDir, streams, pathLister, DEFAULT_CONSOLIDATION_SIZE, extension);
    }

    public static void consolidate(FileSystem fs, String targetDir, RecordStreamFactory streams,
            PathLister pathLister) throws IOException {
        consolidate(fs, targetDir, streams, pathLister, DEFAULT_CONSOLIDATION_SIZE, "");
    }

    public static void consolidate(FileSystem fs, String targetDir, RecordStreamFactory streams,
            PathLister pathLister, long targetSizeBytes, String extension) throws IOException {
        List<String> dirs = new ArrayList<String>();
        dirs.add(targetDir);
        consolidate(fs, streams, pathLister, dirs, targetSizeBytes, extension);
    }

    private static String getDirsString(List<String> targetDirs) {
        String ret = "";
        for(int i=0; i < 3 && i < targetDirs.size(); i++) {
            ret+=(targetDirs.get(i) + " ");
        }
        if(targetDirs.size()>3) {
            ret+="...";
        }
        return ret;
    }

    public static void consolidate(FileSystem fs, RecordStreamFactory streams, PathLister lister, List<String> dirs,
            long targetSizeBytes, String extension) throws IOException {
        JobConf conf = new JobConf(Consolidator.class);
        conf.set("fs.default.name", fs.getUri().toString());
        ConsolidatorArgs args = new ConsolidatorArgs(streams, lister, dirs, targetSizeBytes, extension);
        Utils.setObject(conf, ARGS, args);

        conf.setJobName("Consolidator: " + getDirsString(dirs));
        
        conf.setInputFormat(ConsolidatorInputFormat.class);
        conf.setOutputFormat(NullOutputFormat.class);
        conf.setMapperClass(ConsolidatorMapper.class);

        conf.setSpeculativeExecution(false);

        conf.setNumReduceTasks(0);

        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(NullWritable.class);

        try {
            registerShutdownHook();
            job = new JobClient(conf).submitJob(conf);

            while(!job.isComplete()) {
                Thread.sleep(100);
            }
            if(!job.isSuccessful()) throw new IOException("Consolidator failed");
            deregisterShutdownHook();
        } catch(IOException e) {

            IOException ret = new IOException("Consolidator failed");
            ret.initCause(e);
            throw ret;
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void registerShutdownHook() {
        shutdownHook = new Thread()
        {
            @Override
            public void run()
            {
                try {
                    if(job != null)
                        job.killJob();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        Runtime.getRuntime().addShutdownHook( shutdownHook );
    }

    private static void deregisterShutdownHook()
    {
        Runtime.getRuntime().removeShutdownHook( shutdownHook );
    }

    public static class ConsolidatorMapper extends MapReduceBase implements Mapper<ArrayWritable, Text, NullWritable, NullWritable> {
        public static Logger LOG = Logger.getLogger(ConsolidatorMapper.class);

        FileSystem fs;
        ConsolidatorArgs args;

        public void map(ArrayWritable sourcesArr, Text target, OutputCollector<NullWritable, NullWritable> oc, Reporter rprtr) throws IOException {
            Path finalFile = new Path(target.toString());

            List<Path> sources = new ArrayList<Path>();
            for(int i=0; i<sourcesArr.get().length; i++) {
                sources.add(new Path(((Text)sourcesArr.get()[i]).toString()));
            }
            //must have failed after succeeding to create file but before task finished - this is valid
            //because path is selected with a UUID
            if(!fs.exists(finalFile)) {
                Path tmpFile = new Path("/tmp/consolidator/" + UUID.randomUUID().toString());
                fs.mkdirs(tmpFile.getParent());

                String status = "Consolidating " + sources.size() + " files into " + tmpFile.toString();
                LOG.info(status);
                rprtr.setStatus(status);

                RecordStreamFactory fact = args.streams;
                fs.mkdirs(finalFile.getParent());

                RecordOutputStream os = fact.getOutputStream(fs, tmpFile);
                for(Path i: sources) {
                    LOG.info("Opening " + i.toString() + " for consolidation");
                    RecordInputStream is = fact.getInputStream(fs, i);
                    byte[] record;
                    while((record = is.readRawRecord()) != null) {
                        os.writeRaw(record);
                    }
                    is.close();
                    rprtr.progress();
                }
                os.close();

                status = "Renaming " + tmpFile.toString() + " to " + finalFile.toString();
                LOG.info(status);
                rprtr.setStatus(status);

                if(!fs.rename(tmpFile, finalFile))
                    throw new IOException("could not rename " + tmpFile.toString() + " to " + finalFile.toString());
            }

            String status = "Deleting " + sources.size() + " original files";
            LOG.info(status);
            rprtr.setStatus(status);

            for(Path p: sources) {
                fs.delete(p, false);
                rprtr.progress();
            }
            
        }

        @Override
        public void configure(JobConf job) {
            try {
                fs = FileSystem.get(job);
                args = (ConsolidatorArgs) Utils.getObject(job, ARGS);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }   
    }

    public static class ConsolidatorSplit implements InputSplit {
        public String[] sources;
        public String target;

        public ConsolidatorSplit() {
            
        }

        public ConsolidatorSplit(String[] sources, String target) {
            this.sources = sources;
            this.target = target;
        }


        public long getLength() throws IOException {
            return 1;
        }

        public String[] getLocations() throws IOException {
            return new String[] {};
        }

        public void write(DataOutput d) throws IOException {
            WritableUtils.writeString(d, target);
            WritableUtils.writeStringArray(d, sources);
        }

        public void readFields(DataInput di) throws IOException {
            target = WritableUtils.readString(di);
            sources = WritableUtils.readStringArray(di);
        }
        
    }

    public static class ConsolidatorRecordReader implements RecordReader<ArrayWritable, Text> {
        private ConsolidatorSplit split;
        boolean finished = false;

        public ConsolidatorRecordReader(ConsolidatorSplit split) {
            this.split = split;
        }

        public boolean next(ArrayWritable k, Text v) throws IOException {
            if(finished) return false;
            Writable[] sources = new Writable[split.sources.length];
            for(int i=0; i<sources.length; i++) {
                sources[i] = new Text(split.sources[i]);
            }
            k.set(sources);
            v.set(split.target);

            finished = true;
            return true;
        }

        public ArrayWritable createKey() {
            return new ArrayWritable(Text.class);
        }

        public Text createValue() {
            return new Text();
        }

        public long getPos() throws IOException {
            if(finished) return 1;
            else return 0;
        }

        public void close() throws IOException {
        }

        public float getProgress() throws IOException {
            if(finished) return 1;
            else return 0;
        }
        
    }


    public static class ConsolidatorInputFormat implements InputFormat<ArrayWritable, Text> {

        private static class PathSizePair implements Value {
            public Path path;
            public long size;

            public PathSizePair(Path p, long s) {
                this.path = p;
                this.size = s;
            }

            public long getValue() {
                return size;
            }

        }

        private List<PathSizePair> getFileSizePairs(FileSystem fs, List<Path> files) throws IOException {
            List<PathSizePair> results = new ArrayList<PathSizePair>();
            for(Path p: files) {
                long size = fs.getContentSummary(p).getLength();
                results.add(new PathSizePair(p, size));
            }
            return results;
        }


        private String[] pathsToStrs(List<PathSizePair> pairs) {
            String[] ret = new String[pairs.size()];
            for(int i=0; i<pairs.size(); i++) {
                ret[i] = pairs.get(i).path.toString();
            }
            return ret;
        }

        private List<InputSplit> createSplits(FileSystem fs, List<Path> files,
                String target, long targetSize, String extension) throws IOException {
            List<PathSizePair> working = getFileSizePairs(fs, files);
            List<InputSplit> ret = new ArrayList<InputSplit>();
            List<List<PathSizePair>> splits = SubsetSum.split(working, targetSize);
            for(List<PathSizePair> c: splits) {
                if(c.size()>1) {
                    String rand = UUID.randomUUID().toString();
                    String targetFile = new Path(target,
                            "" + rand.charAt(0) + rand.charAt(1) + "/cons" +
                            rand + extension).toString();
                    ret.add(new ConsolidatorSplit(pathsToStrs(c), targetFile));

                }
            }
            Collections.sort(ret, new Comparator<InputSplit>() {
                public int compare(InputSplit o1, InputSplit o2) {
                    return ((ConsolidatorSplit)o2).sources.length - ((ConsolidatorSplit)o1).sources.length;
                }
            });
            return ret;            
        }

        public InputSplit[] getSplits(JobConf conf, int ignored) throws IOException {
            FileSystem fs = FileSystem.get(conf);
            ConsolidatorArgs args = (ConsolidatorArgs) Utils.getObject(conf, ARGS);
            PathLister lister = args.pathLister;
            List<String> dirs = args.dirs;
            List<InputSplit> ret = new ArrayList<InputSplit>();
            for(String dir: dirs) {
                ret.addAll(createSplits(fs, lister.getFiles(fs,dir),
                        dir, args.targetSizeBytes, args.extension));
            }
            return ret.toArray(new InputSplit[ret.size()]);
        }

        public RecordReader<ArrayWritable, Text> getRecordReader(InputSplit is, JobConf jc, Reporter rprtr) throws IOException {
            return new ConsolidatorRecordReader((ConsolidatorSplit) is);
        }        
    }
}
