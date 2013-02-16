package com.backtype.hadoop.pail;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;


public class AllPailPathLister implements PailPathLister {
    public List<Path> getPaths(Pail p) throws IOException {
        return p.getStoredFiles();
    }

}
