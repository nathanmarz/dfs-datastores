package com.backtype.cascading.tap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.hadoop.TupleSerialization;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailFormatFactory;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;
import com.backtype.support.Utils;

public class PailTap extends Hfs {
    private static Logger LOG = LoggerFactory.getLogger(PailTap.class);

    public static PailSpec makeSpec(PailSpec given, PailStructure structure) {
        return (given == null) ? PailFormatFactory.getDefaultCopy().setStructure(structure) :
                given.setStructure(structure);
    }

    protected String _pailRoot;
    private PailTapOptions _options;

    protected String getCategory(Object obj) {
        return "";
    }

    public PailTap(String root, PailTapOptions options) {
        _options = options;
        setStringPath(root);
        setScheme(new PailScheme(this, options));
        _pailRoot = root;
    }
    
    public String getPailRoot() {
        return _pailRoot;
    }

    public PailTap(String root) {
        this(root, new PailTapOptions());
    }

    @Override
    public String getIdentifier() {
        if (_options.attrs != null && _options.attrs.length > 0) {
            String rel = "";
            for (List<String> attr : _options.attrs) {
                rel += Utils.join(attr, Path.SEPARATOR);
            }
            return getPath().toString() + Path.SEPARATOR + rel;
        } else {
            return getPath().toString();
        }
    }

    @Override
    public boolean deleteResource(JobConf conf) throws IOException {
        throw new UnsupportedOperationException();
    }

    // no good way to override this, just had to copy/paste and modify
    @Override
    public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf) {
        try {
            Path root = getQualifiedPath(conf);
            if (_options.attrs != null && _options.attrs.length > 0) {
                Pail pail = new Pail(_pailRoot);
                for (List<String> attr : _options.attrs) {
                    String rel = Utils.join(attr, "/");
                    pail.getSubPail(rel); // ensure the path exists
                    Path toAdd = new Path(root, rel);
                    LOG.info("Adding input path " + toAdd.toString());
                    FileInputFormat.addInputPath(conf, toAdd);
                }
            } else {
                FileInputFormat.addInputPath(conf, root);
            }

            getScheme().sourceConfInit(process, this, conf);
            makeLocal(conf, getQualifiedPath(conf), "forcing job to local mode, via source: ");
            TupleSerialization.setSerializations(conf);
        } catch (IOException e) {
            throw new TapException(e);
        }
    }

    public List<Path> getPaths() {   	
        final List<Path> paths = new ArrayList<Path>();
        if (_options.attrs != null && _options.attrs.length > 0) {
            for (List<String> attr : _options.attrs) {
                String rel = Utils.join(attr, "/");
                final Path path = new Path(_pailRoot, rel);
                try {
					if( Utils.getFS(path.toString()).exists(path)) {
					    paths.add(path);
					}
				} catch (IOException e) {
					LOG.warn("attributes do not exist for pail " +  rel);
				}
            }
        } else {
            paths.add(new Path(_pailRoot));
        }
        return paths;
    }

    private void makeLocal(JobConf conf, Path qualifiedPath, String infoMessage) {
        if (!conf.get("mapred.job.tracker", "").equalsIgnoreCase("local") && qualifiedPath.toUri()
                .getScheme().equalsIgnoreCase("file")) {
            if (LOG.isInfoEnabled()) {
                LOG.info(infoMessage + toString());
            }

            conf.set("mapred.job.tracker", "local"); // force job to run locally
        }
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
        if (_options.attrs != null && _options.attrs.length > 0) {
            throw new TapException("can't declare attributes in a sink");
        }

        super.sinkConfInit(process, conf);
    }

    @Override
    public boolean commitResource(JobConf conf) throws IOException {
        Pail p = Pail.create(_pailRoot, ((PailScheme) getScheme()).getSpec(), false);
        FileSystem fs = p.getFileSystem();
        Path tmpPath = new Path(_pailRoot, "_temporary");
        if (fs.exists(tmpPath)) {
            LOG.info("Deleting _temporary directory left by Hadoop job: " + tmpPath.toString());
            fs.delete(tmpPath, true);
        }

        Path tmp2Path = new Path(_pailRoot, "_temporary2");
        if (fs.exists(tmp2Path)) {
            LOG.info("Deleting _temporary2 directory: " + tmp2Path.toString());
            fs.delete(tmp2Path, true);
        }

        Path logPath = new Path(_pailRoot, "_logs");
        if (fs.exists(logPath)) {
            LOG.info("Deleting _logs directory left by Hadoop job: " + logPath.toString());
            fs.delete(logPath, true);
        }

        return true;
    }

    @Override
    public int hashCode() {
        return _pailRoot.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (!getClass().equals(object.getClass())) {
            return false;
        }
        PailTap other = (PailTap) object;
        Set<List<String>> myattrs = new HashSet<List<String>>();
        if (_options.attrs != null) {
            Collections.addAll(myattrs, _options.attrs);
        }
        Set<List<String>> otherattrs = new HashSet<List<String>>();
        if (other._options.attrs != null) {
            Collections.addAll(otherattrs, other._options.attrs);
        }
        return _pailRoot.equals(other._pailRoot) && myattrs.equals(otherattrs);
    }

    private Path getQualifiedPath(JobConf conf) throws IOException {
        return getPath().makeQualified(getFileSystem(conf));
    }
}
