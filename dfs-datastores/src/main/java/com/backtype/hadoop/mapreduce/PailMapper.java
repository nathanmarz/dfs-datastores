package com.backtype.hadoop.mapreduce;

import com.backtype.hadoop.pail.PailStructure;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class PailMapper<T, KEYOUT, VALUEOUT> extends Mapper<Text, BytesWritable, KEYOUT, VALUEOUT> {

    private PailStructure<T> pailStructure;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pailStructure = getPailStructure();
    }

    @Override
    protected final void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        processRecord(key, pailStructure.deserialize(value.getBytes()), context);
    }

    protected abstract void processRecord(Text key, T value, Context context) throws IOException, InterruptedException;

    /**
     * We need a PailStructure Implementation to de-serialize the records
     */
    protected abstract PailStructure<T> getPailStructure();
}
