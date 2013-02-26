package com.backtype.hadoop.datastores;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class DefaultTimeSliceStructure extends TimeSliceStructure<byte[]> {
    public byte[] deserialize(byte[] serialized) {
        return serialized;
    }

    public byte[] serialize(byte[] object) {
        return (byte[]) object;
    }

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    @JsonIgnore
    public Class getType() {
        return EMPTY_BYTE_ARRAY.getClass();
    }
}

