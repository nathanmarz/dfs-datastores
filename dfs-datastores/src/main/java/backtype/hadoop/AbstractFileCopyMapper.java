package backtype.hadoop;

import backtype.hadoop.FileCopyInputFormat.FileCopyArgs;
import backtype.support.Utils;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.UUID;

public abstract class AbstractFileCopyMapper extends MapReduceBase implements Mapper<Text, Text, NullWritable, NullWritable> {
    public static Logger LOG = Logger.getLogger(AbstractFileCopyMapper.class);

    private FileSystem fsSource;
    private FileSystem fsDest;
    private String tmpRoot;

    private void setStatus(Reporter rprtr, String msg) {
        LOG.info(msg);
        rprtr.setStatus(msg);
    }

    public void map(Text source, Text target, OutputCollector<NullWritable, NullWritable> oc, Reporter rprtr) throws IOException {
        Path sourceFile = new Path(source.toString());
        Path finalFile = new Path(target.toString());
        Path tmpFile = new Path(tmpRoot, UUID.randomUUID().toString());

        setStatus(rprtr, "Copying " + sourceFile.toString() + " to " + tmpFile.toString());

        if(fsDest.exists(finalFile)) {
            FileChecksum fc1 = fsSource.getFileChecksum(sourceFile);
            FileChecksum fc2 = fsDest.getFileChecksum(finalFile);
            if(fc1 != null && fc2 != null && !fc1.equals(fc2) ||
               fsSource.getContentSummary(sourceFile).getLength()!=fsDest.getContentSummary(finalFile).getLength() ||
               ((fc1==null || fc2==null) && !Utils.firstNBytesSame(fsSource, sourceFile, fsDest, finalFile, 1024*1024))) {
                throw new IOException("Target file already exists and is different! " + finalFile.toString());
            } else {
                return;
            }
        }

        fsDest.mkdirs(tmpFile.getParent());

        copyFile(fsSource, sourceFile, fsDest, tmpFile, rprtr);

        setStatus(rprtr, "Renaming " + tmpFile.toString() + " to " + finalFile.toString());

        fsDest.mkdirs(finalFile.getParent());
        if(!fsDest.rename(tmpFile, finalFile))
            throw new IOException("could not rename " + tmpFile.toString() + " to " + finalFile.toString());

        // this is a bit of a hack; if we don't do this explicit rename, the owner of the file will
        // be hadoop each time.
        //fsDest.setOwner(finalFile, this.owner, fs.getGroup());
    }

    protected abstract void copyFile(FileSystem fsSource, Path source, FileSystem fsDest, Path target, Reporter reporter) throws IOException;

    @Override
    public void configure(JobConf job) {
        FileCopyArgs args = (FileCopyArgs) Utils.getObject(job, FileCopyInputFormat.ARGS);
        try {
            tmpRoot = args.tmpRoot;
            fsSource = new Path(args.source).getFileSystem(job);
            fsDest = new Path(args.dest).getFileSystem(job);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
