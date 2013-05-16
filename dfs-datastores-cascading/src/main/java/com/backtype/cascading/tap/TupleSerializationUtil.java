/**
 * 
 */
package com.backtype.cascading.tap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;

import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.io.HadoopTupleInputStream;
import cascading.tuple.hadoop.io.HadoopTupleOutputStream;
import cascading.tuple.io.TupleInputStream;
import cascading.tuple.io.TupleOutputStream;

class TupleSerializationUtil implements Serializable {
  private static final int BUFFER_SIZE = 4096;
  private final JobConf jobConf;
  private transient TupleSerialization serialization = null;
  private transient ByteArrayOutputStream bytesOutputStream = null;
  private transient TupleOutputStream tupleOutputStream = null;
  private transient Serializer<Tuple> tupleSerializer = null;
  private transient Deserializer<Tuple> tupleDeserializer = null;

  public TupleSerializationUtil(JobConf jobConf) {
    this.jobConf = jobConf;
  }

  public byte[] serialize(Tuple tuple) throws IOException {
    initSerializer();
    bytesOutputStream.reset();
    tupleSerializer.open(tupleOutputStream);
    tupleSerializer.serialize(tuple);
    return bytesOutputStream.toByteArray();
  }

  public Tuple deserialize(byte[] bytes) throws IOException {
    initDeserializer();
    Tuple tuple = new Tuple();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    TupleInputStream tupleInputStream = new HadoopTupleInputStream(inputStream, serialization.getElementReader());
    tupleDeserializer.open(tupleInputStream);
    tupleDeserializer.deserialize(tuple);
    return tuple;
  }

  private void initSerializer() {
    init();
    if (bytesOutputStream == null) {
      bytesOutputStream = new ByteArrayOutputStream(BUFFER_SIZE);
    }
    if (tupleOutputStream == null) {
      tupleOutputStream = new HadoopTupleOutputStream(bytesOutputStream, serialization.getElementWriter());
    }
    if (tupleSerializer == null) {
      tupleSerializer = serialization.getSerializer(Tuple.class);
    }
  }

  private void initDeserializer() {
    init();
    if (tupleDeserializer == null) {
      tupleDeserializer = serialization.getDeserializer(Tuple.class);
    }
  }

  private void init() {
    if (serialization == null) {
      serialization = new TupleSerialization(jobConf);
    }
  }
}