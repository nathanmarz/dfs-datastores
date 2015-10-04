package com.backtype.hadoop;

import com.backtype.hadoop.FileCopyInputFormat.FileCopyArgs;
import com.backtype.support.Utils;
import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;

public class BalancedDistcp {
    private static Thread shutdownHook;
    private static RunningJob job = null;

    public static void distcp(String qualSource, String qualDest, int renameMode, PathLister lister, String extensionOnRename, Configuration conf) throws IOException {
        FileCopyArgs args = new FileCopyArgs(qualSource, qualDest, renameMode, lister, extensionOnRename);
        distcp(args, conf);
    }

    public static void distcp(FileCopyArgs args, Configuration userConf) throws IOException {
        if(!Utils.hasScheme(args.source) || !Utils.hasScheme(args.dest))
            throw new IllegalArgumentException("source and dest must have schemes " + args.source + " " + args.dest);

        JobConf conf;
        if(userConf != null)
            conf = new JobConf(userConf, BalancedDistcp.class);
        else
            conf = new JobConf(BalancedDistcp.class);

        Utils.setObject(conf, FileCopyInputFormat.ARGS, args);

        conf.setJobName("BalancedDistcp: " + args.source + " -> " + args.dest);

        conf.setInputFormat(FileCopyInputFormat.class);
        conf.setOutputFormat(NullOutputFormat.class);
        conf.setMapperClass(BalancedDistcpMapper.class);

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

            if(!job.isSuccessful()) throw new IOException("BalancedDistcp failed");
            deregisterShutdownHook();
        } catch(IOException e) {
            IOException ret = new IOException("BalancedDistcp failed");
            ret.initCause(e);
            throw ret;
        } catch(InterruptedException e) {
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
}
