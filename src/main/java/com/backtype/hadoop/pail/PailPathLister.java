package com.backtype.hadoop.pail;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.fs.Path;

public interface PailPathLister extends Serializable {
    public List<Path> getPaths(Pail p) throws IOException;
}
