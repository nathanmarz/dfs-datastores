package com.backtype.hadoop.pail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * This class exists to get around lack of syncing in Hadoop local filesystem. Collector uses this to create output streams
 */
public abstract class AbstractLocalPail extends AbstractPail {

    private FileSystem _lfs;

    public AbstractLocalPail(String instance_root) throws IOException {
        super(instance_root);
        _lfs = FileSystem.getLocal(new Configuration());
    }

    @Override
    protected boolean delete(Path path, boolean recursive) throws IOException {
        return _lfs.delete(path, recursive);
    }

    @Override
    protected boolean exists(Path path) throws IOException {
        return _lfs.exists(path);
    }

    @Override
    protected boolean rename(Path source, Path dest) throws IOException {
        return _lfs.rename(source, dest);
    }

    @Override
    protected boolean mkdirs(Path path) throws IOException {
        return _lfs.mkdirs(path);
    }

    @Override
    protected FileStatus[] listStatus(Path path) throws IOException {
        return _lfs.listStatus(path);
    }

}
