package com.backtype.hadoop.pail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public class PailPathListerDriver {
    public static void main(String[] args) throws Exception {
        String dir = args[0];
        Path path = new Path(dir);
        Configuration conf = new Configuration();
        FileSystem fs = path.getFileSystem(conf);
        Pail pail = new Pail(fs, dir);
        long start = System.currentTimeMillis();
        System.out.println("Starting to list "+dir);
        List<Path> storedFiles = pail.getStoredFiles();
        System.out.println("Listing path took: "+(System.currentTimeMillis() - start));
        System.out.println("# of files " + storedFiles.size());
        for (Path file : storedFiles) {
            System.out.println("FILE: "+file);
        }

//        long start2 = System.currentTimeMillis();
//        List<Path> storedFilesAndMetadata = pail.getStoredFilesAndMetadata();
//        System.out.println("Listing path took: "+(System.currentTimeMillis() - start2));
//        System.out.println("# of files " + storedFilesAndMetadata.size());
//        for (Path file : storedFilesAndMetadata) {
//            System.out.println("FILE_METADATA: "+file);
//        }
    }
}
