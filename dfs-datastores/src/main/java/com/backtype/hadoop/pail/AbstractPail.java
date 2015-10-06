package com.backtype.hadoop.pail;

import com.backtype.hadoop.formats.RecordInputStream;
import com.backtype.hadoop.formats.RecordOutputStream;
import com.backtype.support.Retry;
import com.backtype.support.Utils;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.util.concurrent.Futures;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import scala.concurrent.ExecutionContext;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;


public abstract class AbstractPail {
    public static final String EXTENSION = ".pailfile";
    public static final String META_EXTENSION = ".metafile";
    public static final String META_TEMP_EXTENSION = ".metafiletmp";
    private static final String TEMP_EXTENSION = ".pailfiletmp";
    private static final Logger LOGGER = Logger.getLogger(AbstractPail.class);

    private class PailOutputStream implements RecordOutputStream {

        private Path tempFile;
        private Path finalFile;
        private RecordOutputStream delegate;

        public PailOutputStream(String userfilename, boolean overwrite) throws IOException {
            tempFile = new Path(_instance_root, userfilename + TEMP_EXTENSION);
            finalFile = new Path(_instance_root, userfilename + EXTENSION);
            if(finalFile.getName().equals(EXTENSION)) throw new IllegalArgumentException("Cannot create empty user file name");

            mkdirs(tempFile.getParent());
            if(exists(tempFile)) {
                delete(tempFile, false);
            }
            delegate = createOutputStream(tempFile);

            if(overwrite && exists(finalFile)) {
                delete(finalFile, false);
            }

            if(exists(finalFile)) {
                delegate.close();
                delete(tempFile, false);
                throw new IOException("File already exists " + finalFile.toString());
            }
        }

        public void writeRaw(byte[] record) throws IOException {
            writeRaw(record, 0, record.length);
        }

        private Function<Object,Boolean> tryRename()
        {
            return new Function<Object, Boolean>() {
                @Override
                public Boolean apply(Object obj) {
                    try {
                        return rename(tempFile, finalFile);
                    } catch (IOException e) {
                        LOGGER.warn("Retrying...");
                    }
                    return false;
                }
            };
        }

        public void close() throws IOException {
            delegate.close();

            if(Retry.retry(3, this, tryRename(), Predicates.<Boolean>equalTo(true)))
                return;
            else
                throw new IOException("Unable to atomically create pailfile with rename " + tempFile.toString());
        }

        @Override
        public void flush() throws IOException {
            // NOT DOING ANYTHING TO LEAVE IT AT STATUS-QUO
        }

        public void writeRaw(byte[] record, int start, int length) throws IOException {
            delegate.writeRaw(record, start, length);
        }
    }

    private String _instance_root;

    public AbstractPail(String path) throws IOException {
        _instance_root = path;
    }

    public boolean exists(String userfilename) throws IOException {
        return exists(toStoredPath(userfilename));
    }

    public RecordOutputStream openWrite(String userfilename) throws IOException {
        return openWrite(userfilename, false);
    }

    public RecordOutputStream openWrite(String userfilename, boolean overwrite) throws IOException {
        return new PailOutputStream(userfilename, overwrite);
    }

    public RecordInputStream openRead(String userfilename) throws IOException {
        return createInputStream(toStoredPath(userfilename));
    }

    public void deleteMetadata(String metafilename) throws IOException {
        Path metaPath = toStoredMetadataPath(metafilename);
        delete(metaPath, false);
    }

    public void mkAttr(String attr) throws IOException {
        mkdirs(new Path(_instance_root + "/" + attr));
    }

    public void writeMetadata(String metafilename, String metadata) throws IOException {
        Path metaPath = toStoredMetadataPath(metafilename);
        Path metaTmpPath = toStoredMetadataTmpPath(metafilename);
        mkdirs(metaTmpPath.getParent());
        delete(metaPath, false);
        delete(metaTmpPath, false);
        RecordOutputStream os = createOutputStream(metaTmpPath);
        os.writeRaw(("M" + metadata).getBytes("UTF-8")); //ensure that it's not an empty record
        os.close();
        rename(metaTmpPath, metaPath);
    }

    public String getMetadata(String metafilename) throws IOException {
        Path metaPath = toStoredMetadataPath(metafilename);
        if(exists(metaPath)) {
            RecordInputStream is = createInputStream(metaPath);
            String metaStr = new String(is.readRawRecord(), "UTF-8");
            is.close();
            return metaStr.substring(1);
        } else {
            return null;
        }
    }

    protected abstract RecordInputStream createInputStream(Path path) throws IOException;
    protected abstract RecordOutputStream createOutputStream(Path path) throws IOException;
    protected abstract boolean delete(Path path, boolean recursive) throws IOException;
    protected abstract boolean exists(Path path) throws IOException;
    protected abstract boolean rename(Path source, Path dest) throws IOException;
    protected abstract boolean mkdirs(Path path) throws IOException;
    protected abstract FileStatus[] listStatus(Path path) throws IOException;

    public List<String> getUserFileNames() throws IOException {
        List<String> ret = new ArrayList<String>();
        getFilesHelper(new Path(_instance_root), "", EXTENSION, true, ret);
        return ret;
    }

    public Path toStoredPath(String userfilename) {
        return new Path(_instance_root, userfilename+EXTENSION);
    }

    public Path toStoredMetadataPath(String metadatafilename) {
        return new Path(_instance_root, metadatafilename+META_EXTENSION);
    }

    public Path toStoredMetadataTmpPath(String metadatafilename) {
        return new Path(_instance_root, metadatafilename+META_TEMP_EXTENSION);
    }


    public void delete(String userfilename) throws IOException {
        delete(toStoredPath(userfilename), false);
    }

    public List<Path> getStoredFiles() throws IOException {
        List<String> userfiles = getUserFileNames();
        List<Path> ret = new ArrayList<Path>();
        for(String u: userfiles) {
            ret.add(toStoredPath(u));
        }
        return ret;
    }


    public List<String> getMetadataFileNames() throws IOException {
        List<String> ret = new ArrayList<String>();
        getFilesHelper(new Path(_instance_root), "", META_EXTENSION, true, ret);
        return ret;
    }

    public List<Path> getStoredMetadataFiles() throws IOException {
        List<String> userfiles = getMetadataFileNames();
        List<Path> ret = new ArrayList<Path>();
        for(String u: userfiles) {
            ret.add(toStoredMetadataPath(u));
        }
        return ret;
    }

    public List<Path> getStoredFilesAndMetadata() throws IOException {
        List<String> relFiles = new ArrayList<String>();
        List<String> extensions = new ArrayList<String>();
        extensions.add(META_EXTENSION);
        extensions.add(EXTENSION);
        getFilesHelperOptimized(new Path(_instance_root), "", extensions, false, relFiles);
        List<Path> ret = new ArrayList<Path>();
        for(String rel: relFiles) {
            ret.add(new Path(_instance_root, rel));
        }
        return ret;
    }


    public List<Path> getStoredUnfinishedFiles() throws IOException {
        List<String> userfiles = new ArrayList<String>();
        getFilesHelper(new Path(_instance_root), "", TEMP_EXTENSION, true, userfiles);
        List<Path> ret = new ArrayList<Path>();
        for(String u: userfiles) {
            ret.add(new Path(_instance_root, u+TEMP_EXTENSION));
        }
        return ret;
    }

    protected List<String> readDir(String subdir, boolean dir) throws IOException {
        Path absDir;
        if(subdir.length()==0) {
            absDir = new Path(_instance_root);
        } else {
            absDir = new Path(_instance_root, subdir);
        }

        List<String> ret = new ArrayList<String>();
        FileStatus[] contents = listStatus(absDir);
        for(FileStatus fs: contents) {
            String name = fs.getPath().getName();
            if((fs.isDir() && dir || !fs.isDir() && !dir) && !name.contains("_")) {
                ret.add(name);
            }
        }
        return ret;
    }

    public List<String> getAttrsAtDir(String subdir) throws IOException {
        return readDir(subdir, true);
    }

    public List<String> getMetadataFileNames(String subdir) throws IOException {
        List<String> files = readDir(subdir, false);
        List<String> ret = new ArrayList<String>();
        for(String f: files) {
            if(f.endsWith(META_EXTENSION)) {
                ret.add(Utils.stripExtension(f, META_EXTENSION));
            }
        }
        return ret;
    }

    public String getInstanceRoot() {
        return _instance_root;
    }

    private String relify(String root, String name) {
        if(root.length()==0) return name;
        else return new Path(root, name).toString();
    }

    private void getFilesHelper(Path abs, String rel, String extension, boolean stripExtension, List<String> files) throws IOException {
        List<String> extensions = new ArrayList<String>();
        extensions.add(extension);
        getFilesHelperOptimized(abs, rel, extensions, stripExtension, files);
    }

//    TODO: This should go away after we find it stable.
    private void getFilesHelper(Path abs, String rel, List<String> extensions, boolean stripExtension, List<String> files) throws IOException {
        FileStatus[] contents = listStatus(abs);
        for(FileStatus stat: contents) {
            Path p = stat.getPath();
            if(stat.isDir()) {
                getFilesHelper(p, relify(rel, stat.getPath().getName()), extensions, stripExtension, files);
            } else {
                String filename = relify(rel, stat.getPath().getName());
                for(String extension: extensions) {
                    if(filename.endsWith(extension) && stat.getLen()>0) {
                        String toAdd;
                        if(stripExtension) {
                            toAdd = Utils.stripExtension(filename, extension);
                        } else {
                            toAdd = filename;
                        }
                        files.add(toAdd);
                        break;
                    }
                }
            }
        }
    }

    private Path withoutScheme(Path path){
        String rawPath = path.toUri().getRawPath();
        return new Path(rawPath);
    }

    private String relativizePaths(Path base, Path itemToRelativize){
        URI baseUri = base.toUri();
        if(baseUri.getScheme() == null){
            String rawBasePath = baseUri.getRawPath();
            Path p = withoutScheme(itemToRelativize);
            return p.toString().replaceFirst(rawBasePath,"").replaceFirst("/","");
        }else{
            return itemToRelativize.toString().replaceFirst(base.toString(), "").replaceFirst("/","");
        }
    }

    private void getFilesHelperOptimized(Path abs, String rel, List<String> extensions, boolean stripExtension, List<String> files) throws IOException {
        ThreadPoolExecutor executorService = (ThreadPoolExecutor)Executors.newFixedThreadPool(16);
        BlockingQueue<FileStatus> outQ = new LinkedBlockingQueue<FileStatus>(10000);
        List<FileStatus> items = Arrays.asList(listStatus(abs));
        LOGGER.info("Start enlisting base dir " + abs);
        long startFileListing = System.currentTimeMillis();
        for (FileStatus item : items) {
            outQ.add(item);
        }
        int tasksSubmitted = 0;
        do {
            FileStatus item = outQ.poll();
            if(item == null) continue;
            if (!item.isDir()) {
                String filename = relativizePaths(abs, item.getPath());
                for (String extension : extensions) {
                    if (filename.endsWith(extension) && item.getLen() > 0) {
                        String toAdd = stripExtension ? Utils.stripExtension(filename, extension) : filename;
                        files.add(toAdd);
                        break;
                    }
                }
            } else {
                executorService.submit(fetchFiles(outQ, item.getPath()));
                ++tasksSubmitted;
            }
        } while (executorService.getCompletedTaskCount() < tasksSubmitted || !outQ.isEmpty());
        executorService.shutdown();
        LOGGER.info("Total # of files under " + abs.toString() + " = " + files.size() + ", took: " + (System.currentTimeMillis() - startFileListing) + " millis");
    }

    /*Pass the base dir to be relified inside. The item in the outQ should be relified wrt to its base*/
    private Runnable fetchFiles(final BlockingQueue<FileStatus> queue, final Path absPath) throws IOException {
        return new Runnable() {
            public void run() {
                try {
                    LOGGER.debug("Fetching files under " + absPath);
                    long start = System.currentTimeMillis();
                    FileStatus[] items = listStatus(absPath);
                    LOGGER.debug("Got " + items.length + " for " + absPath + ", in " + (System.currentTimeMillis()-start)+" millis");
                    for (FileStatus item : items) {
                        queue.put(item);
                    }
                } catch (IOException e) {
                    LOGGER.error(e);
                } catch (InterruptedException e) {
                    LOGGER.error(e);
                }
            }
        };
    }

}
