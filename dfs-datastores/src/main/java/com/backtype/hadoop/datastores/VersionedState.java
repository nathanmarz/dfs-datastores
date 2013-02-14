package com.backtype.hadoop.datastores;

import com.backtype.support.Utils;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;


/**
 * A simple, durable, atomic K/V database. *Very inefficient*, should only be used for occasional reads/writes.
 * Every read/write hits disk.
 */
public class VersionedState {
    private VersionedStore _vs;

    public VersionedState(String backingDir) throws IOException {
        _vs = new VersionedStore(backingDir);
    }

    public synchronized Map<Object, Object> snapshot() throws IOException {
        String latestPath = _vs.mostRecentVersionPath();
        if(latestPath==null) return new HashMap<Object, Object>();
        return (Map<Object, Object>) Utils.deserialize(readFile(latestPath));
    }

    public Object get(Object key) throws IOException {
        return snapshot().get(key);
    }

    public synchronized void put(Object key, Object val) throws IOException {
        Map<Object, Object> curr = snapshot();
        curr.put(key, val);
        persist(curr);
    }

    private void persist(Map<Object, Object> val) throws IOException {
        byte[] toWrite = Utils.serialize(val);
        String newPath = _vs.createVersion();
        FSDataOutputStream os = _vs.getFileSystem().create(new Path(newPath));
        os.write(toWrite);
        os.close();
        _vs.succeedVersion(newPath);
        _vs.cleanup(4);
    }

    private byte[] readFile(String path) throws IOException {
        FSDataInputStream in = _vs.getFileSystem().open(new Path(path));
        byte[] ret = Utils.readFully(in);
        in.close();
        return ret;
    }
}
