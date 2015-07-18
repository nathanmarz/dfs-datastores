package com.backtype.hadoop;

import com.backtype.hadoop.formats.RecordInputStream;
import com.backtype.hadoop.formats.RecordOutputStream;
import com.backtype.hadoop.formats.RecordStreamFactory;
import com.backtype.hadoop.pail.PailStructure;
import com.backtype.support.SubsetSum;
import com.backtype.support.SubsetSum.Value;
import com.backtype.support.Utils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;


public class Consolidator {
    public static final long DEFAULT_CONSOLIDATION_SIZE = 1024*1024*1024*2l; //2G
    private static final String ARGS = "consolidator_args";

    private static Thread shutdownHook;
    private static RunningJob job = null;

    public static class ConsolidatorArgs implements Serializable {
        public String fsUri;
        public RecordStreamFactory streams;
        public PathLister pathLister;
        public List<String> dirs;
        public long targetSizeBytes;
        public String extension;
        public PailStructure<?> structure;
        public String rootDir;


        public ConsolidatorArgs(String fsUri, RecordStreamFactory streams, PathLister pathLister,
                                List<String> dirs, long targetSizeBytes, String extension, PailStructure<?> structure, String rootDir) {
            this.fsUri = fsUri;
            this.streams = streams;
            this.pathLister = pathLister;
            this.dirs = dirs;
            this.targetSizeBytes = targetSizeBytes;
            this.extension = extension;
            this.structure = structure;
            this.rootDir = rootDir;
        }
    }

    public static void consolidate(FileSystem fs, String targetDir, RecordStreamFactory streams,
        PathLister pathLister, long targetSizeBytes, PailStructure<?> structure, String rootDir) throws IOException {
        consolidate(fs, targetDir, streams, pathLister, targetSizeBytes, "", structure, rootDir);
    }

    public static void consolidate(FileSystem fs, String targetDir, RecordStreamFactory streams,
        PathLister pathLister, String extension, PailStructure<?> structure, String rootDir) throws IOException {
        consolidate(fs, targetDir, streams, pathLister, DEFAULT_CONSOLIDATION_SIZE, extension, structure, rootDir);
    }

    public static void consolidate(FileSystem fs, String targetDir, RecordStreamFactory streams,
        PathLister pathLister, PailStructure<?> structure, String rootDir) throws IOException {
        consolidate(fs, targetDir, streams, pathLister, DEFAULT_CONSOLIDATION_SIZE, "", structure, rootDir);
    }

    public static void consolidate(FileSystem fs, String targetDir, RecordStreamFactory streams, PathLister pathLister,
                                   long targetSizeBytes, String extension, PailStructure<?> structure, String rootDir) throws IOException {
        List<String> dirs = new ArrayList<String>();
        dirs.add(targetDir);
        consolidate(fs, streams, pathLister, dirs, targetSizeBytes, extension, structure, rootDir);
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
                                   long targetSizeBytes, String extension, PailStructure<?> structure, String rootDir) throws IOException {
        JobConf conf = new JobConf(fs.getConf(), Consolidator.class);
        String fsUri = fs.getUri().toString();
        ConsolidatorArgs args = new ConsolidatorArgs(fsUri, streams, lister, dirs, targetSizeBytes, extension, structure, rootDir);
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
        public static Logger LOG = LoggerFactory.getLogger(ConsolidatorMapper.class);

        FileSystem fs;
        ConsolidatorArgs args;

        public void map(ArrayWritable sourcesArr, Text target, OutputCollector<NullWritable, NullWritable> oc, Reporter rprtr) throws IOException {

            Path finalFile = new Path(target.toString());

            List<Path> sources = new ArrayList<Path>();
            Set<String> parents = new HashSet<String>();
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
                    parents.add(i.getParent().toString());
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

            for(String p: parents) {
                Path path = new Path(p);
                if(fs.listStatus(path).length == 0) {
                    LOG.info("Deleting consolidated directory " + p);
                    fs.delete(path, true);
                }
                rprtr.progress();
            }

        }

        @Override
        public void configure(JobConf conf) {
            args = (ConsolidatorArgs) Utils.getObject(conf, ARGS);
            try {
                fs = Utils.getFS(args.fsUri, conf);
            } catch(IOException e) {
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

        private Map<String, List<Path>> groupByParentPaths(PailStructure structure, List<Path> files, String pailRoot) {
            HashMap<String, List<Path>> results = new HashMap<String, List<Path>>();
            for (Path file : files) {
                String parentLocation = getParent(structure, file, pailRoot);
                if (results.containsKey(parentLocation)) {
                    results.get(parentLocation).add(file);
                } else {
                    ArrayList<Path> list = new ArrayList<Path>();
                    list.add(file);
                    results.put(parentLocation, list);
                }
            }

            return results;
        }

        private String getParent(PailStructure<?> structure, Path path, String pailRoot) {
            Path lastParentPath = path;

            boolean isValid = true;
            boolean withinPailRoot = true;
            while (lastParentPath.getParent() != null && isValid && withinPailRoot) {
                Path tempParent = lastParentPath.getParent();
                String relative = Utils.makeRelative(new Path(pailRoot), tempParent);
                withinPailRoot = !relative.equals("");
                isValid = withinPailRoot ?
                        structure.isValidTarget(Utils.componentize(relative).toArray(new String[0])) :
                        structure.isValidTarget(relative);
                // System.out.println("isValid - " + tempParent.toString() + " is " + isValid);
                if(isValid) lastParentPath = tempParent;
            }

            // System.out.println("Returning parent for " + path + " as " + lastParentPath);
            return lastParentPath.toString();
        }

        private List<InputSplit> createSplits(FileSystem fs, List<Path> files,
                                              long targetSize, String extension, PailStructure<?> structure, String pailRoot) throws IOException {
            Map<String, List<Path>> grouped = groupByParentPaths(structure, files, pailRoot);
            List<InputSplit> ret = new ArrayList<InputSplit>();

            for (String parentDir : grouped.keySet()) {
                List<PathSizePair> working = getFileSizePairs(fs, grouped.get(parentDir));
                List<List<PathSizePair>> splits = SubsetSum.split(working, targetSize);
                for (List<PathSizePair> c : splits) {
                    if (c.size() > 1) {
                        String rand = UUID.randomUUID().toString();
                        String targetFile = new Path(parentDir,
                                "" + rand.charAt(0) + rand.charAt(1) + "/cons" +
                                        rand + extension).toString();
                        ret.add(new ConsolidatorSplit(pathsToStrs(c), targetFile));

                    }
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
            ConsolidatorArgs args = (ConsolidatorArgs) Utils.getObject(conf, ARGS);
            PathLister lister = args.pathLister;
            List<String> dirs = args.dirs;
            List<InputSplit> ret = new ArrayList<InputSplit>();
            for(String dir: dirs) {
                FileSystem fs = Utils.getFS(dir, conf);
                ret.addAll(createSplits(fs, lister.getFiles(fs, dir),
                        args.targetSizeBytes, args.extension, args.structure, args.rootDir));
            }
            return ret.toArray(new InputSplit[ret.size()]);
        }

        public RecordReader<ArrayWritable, Text> getRecordReader(InputSplit is, JobConf jc, Reporter rprtr) throws IOException {
            return new ConsolidatorRecordReader((ConsolidatorSplit) is);
        }
    }
}
