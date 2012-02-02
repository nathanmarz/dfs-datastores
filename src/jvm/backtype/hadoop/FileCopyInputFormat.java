package backtype.hadoop;

import backtype.support.SubsetSum;
import backtype.support.SubsetSum.Value;
import backtype.support.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class FileCopyInputFormat implements InputFormat<Text, Text> {
    public static final String ARGS = "file_copy_args";
    public static final String WORK_PER_WORKER = "file_copy_work";
    public static final long DEFAULT_WORK_PER_WORKER = 256*1024*1024;


    public static class FileCopyArgs implements Serializable {
        public String source;
        public String dest;
        public PathLister lister;
        int renameMode;
        public String renamableExtension;
        public boolean allToRoot = false;

        public FileCopyArgs(String source, String dest, int renameMode, PathLister lister, String renamableExtension) {
            this.source = source;
            this.dest = dest;
            this.lister = lister;
            this.renamableExtension = renamableExtension;
            this.renameMode = renameMode;
            if(this.renamableExtension==null) this.renamableExtension = "";
        }
    }

    public static class FileCopy {
        public String source;
        public String target;

        public FileCopy(String source, String target) {
            this.source = source;
            this.target = target;
        }
    }

    public static class FileCopySplit implements InputSplit {

        public List<FileCopy> copies;

        public FileCopySplit() {
        }

        public FileCopySplit(List<FileCopy> copies) {
            this.copies = copies;
        }

        public long getLength() throws IOException {
            return copies.size();
        }

        public String[] getLocations() throws IOException {
            return new String[]{};
        }

        public void write(DataOutput d) throws IOException {
            d.writeInt(copies.size());
            for (FileCopy copy : copies) {
                WritableUtils.writeString(d, copy.source);
                WritableUtils.writeString(d, copy.target);
            }
        }

        public void readFields(DataInput di) throws IOException {
            int size = di.readInt();
            copies = new ArrayList<FileCopy>(size);
            for (int i = 0; i < size; i++) {
                String source = WritableUtils.readString(di);
                String target = WritableUtils.readString(di);
                copies.add(new FileCopy(source, target));
            }
        }
    }

    public static class FileCopyRecordReader implements RecordReader<Text, Text> {

        private FileCopySplit split;
        int pos = 0;

        public FileCopyRecordReader(FileCopySplit split) {
            this.split = split;
        }

        public boolean next(Text k, Text v) throws IOException {
            if (pos >= split.copies.size()) {
                return false;
            }
            FileCopy copy = split.copies.get(pos);
            k.set(copy.source);
            v.set(copy.target);
            pos++;
            return true;
        }

        public Text createKey() {
            return new Text();
        }

        public Text createValue() {
            return new Text();
        }

        public long getPos() throws IOException {
            return pos;
        }

        public void close() throws IOException {
        }

        public float getProgress() throws IOException {
            return (float) (1.0 * pos / split.copies.size());
        }
    }

    private static class FileCopyAndSize implements Value {
        FileCopy copy;
        long size;

        public FileCopyAndSize(String source, String target, long size) {
            this.copy = new FileCopy(source, target);
            this.size = size;
        }

        public long getValue() {
            return size;
        }        
    }

    private List<FileCopy> getFileCopies(List<FileCopyAndSize> c) {
        List<FileCopy> ret = new ArrayList<FileCopy>();
        for(FileCopyAndSize f: c) {
            ret.add(f.copy);
        }
        return ret;
    }

    public InputSplit[] getSplits(JobConf conf, int mappers) throws IOException {
        FileCopyArgs args = (FileCopyArgs) Utils.getObject(conf, ARGS);
        if(args.allToRoot && args.renameMode != RenameMode.ALWAYS_RENAME) {
            throw new IllegalArgumentException("Must rename paths if sending all to root");
        }
        FileSystem fsSource = new Path(args.source).getFileSystem(new Configuration());
        FileSystem fsDest = new Path(args.dest).getFileSystem(new Configuration());
        long workPerWorker = conf.getLong(WORK_PER_WORKER, DEFAULT_WORK_PER_WORKER);
        List<FileCopyAndSize> all = new ArrayList<FileCopyAndSize>();
        List<Path> fullPaths = args.lister.getFiles(fsSource, args.source);
        for(Path p: fullPaths) {
            long size = fsSource.getContentSummary(p).getLength();
            Path destp;
            if(args.allToRoot) {
                destp = new Path(args.dest, p.getName());
            } else {
                destp = new Path(args.dest, Utils.makeRelative(new Path(args.source), p));
            }
            String dest;
            boolean targetExists = fsDest.exists(destp);
            if(targetExists || args.renameMode==RenameMode.ALWAYS_RENAME) {
                if(args.renameMode != RenameMode.NO_RENAME && p.getName().endsWith(args.renamableExtension)) {
                    dest = new Path(destp.getParent(), "fc_" + UUID.randomUUID().toString() + args.renamableExtension).toString();
                } else {
                    if(!targetExists) {
                        dest = destp.toString();
                    } else {
                        throw new IllegalArgumentException("File already exists and can't rename source: " + p.toString() + " -> " + destp.toString());
                    }
                }
            } else {
                dest = destp.toString();
            }
            all.add(new FileCopyAndSize(p.toString(), dest, size));
        }
        List<List<FileCopyAndSize>> splits;
        if(all.size() < 10) { //speed up small appends
            splits = new ArrayList<List<FileCopyAndSize>>();
            for(FileCopyAndSize f: all) {
                List<FileCopyAndSize> l = new ArrayList<FileCopyAndSize>();
                l.add(f);
                splits.add(l);
            }
        } else {
            splits = SubsetSum.split(all, workPerWorker);
        }
        InputSplit[] ret = new InputSplit[splits.size()];
        for(int i=0; i<splits.size(); i++) {
            ret[i] = new FileCopySplit(getFileCopies(splits.get(i)));
        }

        return ret;
    }

    public RecordReader<Text, Text> getRecordReader(InputSplit is, JobConf jc, Reporter rprtr) throws IOException {
        return new FileCopyRecordReader((FileCopySplit) is);
    }
}
