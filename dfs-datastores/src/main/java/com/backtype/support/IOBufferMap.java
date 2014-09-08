package com.backtype.support;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import com.backtype.hadoop.formats.RecordOutputStream;

public class IOBufferMap extends LinkedHashMap<String,RecordOutputStream> {
    final int capacity;

    public IOBufferMap(int size) {
        this.capacity = size;
    }

    protected boolean removeEldestEntry(Map.Entry<String,RecordOutputStream> eldest) {
        boolean shouldRemove = (size() >= capacity);
        if(shouldRemove){
            try {
                eldest.getValue().flush();
            } catch (IOException e) {
                // Do we throw it up?
                e.printStackTrace();
            }
        }
        return false;
    }
}
