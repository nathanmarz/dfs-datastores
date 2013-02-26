package com.backtype.hadoop.pail;

public class SequenceFileFormatTest extends PailFormatTester {

    public SequenceFileFormatTest() throws Exception {
        super();
    }

    @Override
    protected PailSpec getSpec() {
        return new PailSpec("SequenceFile");
    }

}
