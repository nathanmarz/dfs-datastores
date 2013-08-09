package com.backtype.hadoop.pail;

public class SequenceFileFormatCompressedTest extends PailFormatTester {

    public SequenceFileFormatCompressedTest() throws Exception {
        super();
    }

    @Override
    protected PailSpec getSpec() {
        return new PailSpec("SequenceFile").setArg("compressionType", "record").setArg("compressionCodec", "default");
    }

}
