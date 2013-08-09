package com.backtype.support;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.backtype.hadoop.datastores.VersionedStore;


public class CascadingUtils {
    public static void identityFlow(Tap source, Tap sink, Fields selectFields) {
        Pipe pipe = new Pipe("pipe");
        pipe = new Each(pipe, selectFields, new Identity());
        Flow flow = new HadoopFlowConnector().connect(source, sink, pipe);
        flow.complete();
    }

    // Mark the output dir of the job for which the context is passed.
    public static void markSuccessfulOutputDir(Path path, JobConf conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        // create a file in the folder to mark it
        if (fs.exists(path)) {
            Path filePath = new Path(path, VersionedStore.HADOOP_SUCCESS_FLAG);
            fs.create(filePath).close();
        }
    }

    public static boolean isSinkOf(Tap tap, Flow flow) {
        for(Object t: flow.getSinksCollection()) {
            if(t==tap) return true;
        }
        return false;
    }
}
