package com.backtype.hadoop.mapreduce;

import com.backtype.hadoop.mapreduce.io.PailOutputFormat;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;
import com.backtype.support.Utils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class PailMapper<T, KEYOUT, VALUEOUT> extends Mapper<Text, BytesWritable, KEYOUT, VALUEOUT> {

    private PailStructure<T> pailStructure;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        PailSpec pailSpec = (PailSpec)Utils.getObject(context.getConfiguration(), PailOutputFormat.SPEC_ARG);
        pailStructure = pailSpec.getStructure();
    }

    @Override
    protected final void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        processRecord(key, pailStructure.deserialize(value.getBytes()), context);
    }

    protected abstract void processRecord(Text key, T value, Context context) throws IOException, InterruptedException;
}
