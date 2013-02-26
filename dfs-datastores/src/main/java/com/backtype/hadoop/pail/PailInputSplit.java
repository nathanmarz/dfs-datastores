package com.backtype.hadoop.pail;

import com.backtype.support.Utils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class PailInputSplit extends FileSplit {

    private String[] _hosts;
    private PailSpec _spec;
    private String _relPath;

    public PailInputSplit() {
        super(null, 0, 0, (String[]) null);
    }

    public PailInputSplit(FileSystem fs, String root, PailSpec spec, JobConf job, FileSplit split) throws IOException {
        super(split.getPath(), split.getStart(), split.getLength(), (String[])null);
        _spec = spec;
        _hosts = split.getLocations();
        setRelPath(fs, root);
    }

    private void setRelPath(FileSystem fs, String root) {
        Path filePath = super.getPath();
        filePath = filePath.makeQualified(fs);
        Path rootPath = new Path(root).makeQualified(fs);

        List<String> dirs = new LinkedList<String>();
        Path curr = filePath.getParent();
        while(!curr.equals(rootPath)) {
            dirs.add(0, curr.getName());
            curr = curr.getParent();
            if(curr==null) throw new IllegalArgumentException(filePath.toString() + " is not a subpath of " + rootPath.toString());
        }
        _relPath = Utils.join(dirs, "/");
    }

    public String getPailRelPath() {
        return _relPath;
    }

    public PailSpec getSpec() {
        return _spec;
    }

    @Override
    public String[] getLocations() throws IOException {
        return _hosts;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        _spec.write(out);
        WritableUtils.writeString(out, _relPath);
        super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        _spec = new PailSpec();
        _spec.readFields(in);

        _relPath = WritableUtils.readString(in);
        super.readFields(in);
    }
}
