package com.backtype.hadoop.pail;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.jvyaml.YAML;

import com.backtype.hadoop.pail.DefaultPailStructure;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


public class PailSpec implements Writable, Serializable {
    private String name;
    private Map<String, Object> args;
    private PailStructure structure;
        
    static final private ObjectMapper mObjectMapper = new ObjectMapper();
    
    static {
        mObjectMapper.registerModule(new JodaModule());
        mObjectMapper.registerModule(new CascadingModule());
        mObjectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }

    private static final PailStructure DEFAULT_STRUCTURE = new DefaultPailStructure();

    public PailSpec() {
        
    }

    public PailSpec(String name) {
        this(name, (PailStructure)null);
    }

    public PailSpec(String name, PailStructure structure) {
        this(name, new HashMap<String, Object>(), structure);
    }

    public PailSpec(String name, Map<String, Object> args) {
        this(name, args, null);
    }

    public PailSpec(String name, Map<String, Object> args, PailStructure structure) {
        this.name = name;
        this.args = args == null ? null : new HashMap(args);
        this.structure = structure;
    }

    public PailSpec(PailStructure structure) {
        this(null, null, structure);
    }

    public PailSpec setStructure(PailStructure structure) {
        this.structure = structure;
        return this;
    }

    public PailSpec setArg(String arg, Object val) {
        this.args.put(arg, val);
        return this;
    }

    @Override
    public String toString() {
        return mapify().toString();
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof PailSpec)) return false;
        PailSpec ps = (PailSpec) obj;
        return name.equals(ps.name) &&
               args.equals(ps.args) &&
               getStructure().getClass().equals(ps.getStructure().getClass());
    }

    @Override
    public int hashCode() {
        return name.hashCode() + args.hashCode();
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> getArgs() {
        return args;
    }

    public PailStructure getStructure() {
        if(structure == null) return DEFAULT_STRUCTURE;
        else return structure;
    }
    

    public static PailSpec readFromFileSystem(FileSystem fs, Path path) throws IOException {
        FSDataInputStream is = fs.open(path);
        PailSpec ret = parseFromStream(is);
        is.close();
        return ret;
    }

    public static PailSpec parseFromStream(InputStream is) {
        Map format = (Map) YAML.load(new InputStreamReader(is));
        return parseFromMap(format);
    }

    protected static PailStructure getStructureFromClass(String klass) {
        if(klass==null) return null;
        Class c;
        try {
            c = Class.forName(klass);
            return (PailStructure) c.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate PailStructure class " + klass, e);
        }
    }

    protected static PailSpec parseFromMap(Map<String, Object> format) {
        String name = (String) format.get("format");
        Map<String, Object> args = (Map<String, Object>) format.get("args");
        String structClass = (String) format.get("structure");
        return new PailSpec(name, args, deserializePailStructure(structClass));
    }

    public void writeToStream(OutputStream os) {
        YAML.dump(mapify(), new OutputStreamWriter(os));
    }
    
    static private PailStructure deserializePailStructure(String value) {    
        try {
            return getStructureFromClass(value);
        } catch (Exception e) {
            try {
                return mObjectMapper.readValue(value, PailStructure.class);
            } catch (IOException e1) {
                throw new RuntimeException("cannot deserialize " + value + ": " + e1.getMessage());
            }
        }
    }
    
    private String serializePailStructure() {
        try {
            String serialized = mObjectMapper.writeValueAsString(structure);
            return serialized;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> mapify() {
        Map<String, Object> format = new HashMap<String, Object>();
        format.put("format", name);
        format.put("args", args);
        if(structure!=null) {
            format.put("structure", serializePailStructure() );
        }
        return format;
    }

    public void writeToFileSystem(FileSystem fs, Path path) throws IOException {
        FSDataOutputStream os = fs.create(path);
        writeToStream(os);
        os.close();
    }

    public void write(DataOutput d) throws IOException {
        String ser = YAML.dump(mapify());
        WritableUtils.writeString(d, ser);
    }

    public void readFields(DataInput di) throws IOException {
        PailSpec spec = parseFromMap((Map<String, Object>)YAML.load(WritableUtils.readString(di)));
        this.name = spec.name;
        this.args = spec.args;
    }
}
