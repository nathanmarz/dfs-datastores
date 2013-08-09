package com.backtype.hadoop.pail;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;


public class AllPailPathLister implements PailPathLister {
    public List<Path> getPaths(Pail p) throws IOException {
        return p.getStoredFiles();
    }

}
