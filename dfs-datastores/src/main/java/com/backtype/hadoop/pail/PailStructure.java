package com.backtype.hadoop.pail;

import java.io.Serializable;
import java.util.List;

/**
 * Shouldn't take any args
 */
public interface PailStructure<T> extends Serializable {
    public boolean isValidTarget(String... dirs);
    public T deserialize(byte[] serialized);
    public byte[] serialize(T object);
    public List<String> getTarget(T object);
    public Class getType();
}
