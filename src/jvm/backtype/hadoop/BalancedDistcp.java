package backtype.hadoop;

import backtype.hadoop.FileCopyInputFormat.FileCopyArgs;
import backtype.support.Utils;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class BalancedDistcp {
    public static void distcp(String qualifiedSource, String qualifiedDest, int renameMode, PathLister lister) throws IOException {
        distcp(qualifiedSource, qualifiedDest, renameMode, lister, "");
    }

    public static void distcp(String qualSource, String qualDest, int renameMode, PathLister lister, String extensionOnRename) throws IOException {
        if(!Utils.hasScheme(qualSource) || !Utils.hasScheme(qualDest))
            throw new IllegalArgumentException("source and dest must have schemes " + qualSource + " " + qualDest);

        FileCopyArgs args = new FileCopyArgs(qualSource, qualDest, renameMode, lister, extensionOnRename);
        JobConf conf = new JobConf(BalancedDistcp.class);
        Utils.setObject(conf, FileCopyInputFormat.ARGS, args);

        conf.setJobName("BalancedDistcp: " + qualSource + " -> " + qualDest);

        conf.setInputFormat(FileCopyInputFormat.class);
        conf.setOutputFormat(NullOutputFormat.class);
        conf.setMapperClass(BalancedDistcpMapper.class);

        conf.setSpeculativeExecution(false);

        conf.setNumReduceTasks(0);

        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(NullWritable.class);

        RunningJob job = null;
        
        try {
            job = new JobClient(conf).submitJob(conf);

            while(!job.isComplete()) {
                Thread.sleep(100);
            }

            if(!job.isSuccessful()) throw new IOException("BalancedDistcp failed");
        } catch(IOException e) {
            if (job!=null) job.killJob();

            IOException ret = new IOException("BalancedDistcp failed");
            ret.initCause(e);
            throw ret;
        } catch(InterruptedException e) {
            job.killJob();
            throw new RuntimeException(e);
        }
    }
    
    public static class BalancedDistcpMapper extends AbstractFileCopyMapper {
        byte[] buffer = new byte[128*1024]; //128 K

        @Override
        protected void copyFile(FileSystem fsSource, Path source, FileSystem fsDest, Path target, Reporter reporter) throws IOException {
            FSDataInputStream fin = fsSource.open(source);
            FSDataOutputStream fout = fsDest.create(target);
            try {
                int amt;
                while((amt = fin.read(buffer)) >= 0) {
                    fout.write(buffer, 0, amt);
                    reporter.progress();
                }
            } finally {
                fin.close();
            }
            //don't complete files that aren't done yet. prevents partial files from being written
            //doesn't really matter though since files are written to tmp file and renamed
            fout.close();
        }
    }
}
