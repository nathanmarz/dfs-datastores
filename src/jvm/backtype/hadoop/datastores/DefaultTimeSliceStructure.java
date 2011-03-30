package backtype.hadoop.datastores;


public class DefaultTimeSliceStructure extends TimeSliceStructure<byte[]> {
    public byte[] deserialize(byte[] serialized) {
        return serialized;
    }

    public byte[] serialize(byte[] object) {
        return (byte[]) object;
    }

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public Class getType() {
        return EMPTY_BYTE_ARRAY.getClass();
    }
}
