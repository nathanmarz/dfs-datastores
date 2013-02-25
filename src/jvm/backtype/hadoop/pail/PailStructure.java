package backtype.hadoop.pail;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Shouldn't take any args
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include=JsonTypeInfo.As.PROPERTY, property="@class")
public interface PailStructure<T> extends Serializable {
    public boolean isValidTarget(String... dirs);
    public T deserialize(byte[] serialized);
    public byte[] serialize(T object);
    public List<String> getTarget(T object);
    public Class getType();
}