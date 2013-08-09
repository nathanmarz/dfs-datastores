package com.backtype.hadoop;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Class to use so you don't have to stuff tons of stuff in the JobConf.
 *
 */
public interface PathLister extends Serializable {
    public List<Path> getFiles(FileSystem fs, String path); 
}
