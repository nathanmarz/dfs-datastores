package com.backtype.hadoop.pail;

import com.backtype.support.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PailFormatFactory {
    public static final String SEQUENCE_FILE = "SequenceFile";

    public static final String PAIL_PATH_LISTER = "pail.path.lister";

    public static void setPailPathLister(Configuration conf, PailPathLister lister) {
        Utils.setObject(conf, PAIL_PATH_LISTER, lister);
    }

    public static List<Path> getPailPaths(Pail p, Configuration conf) throws IOException {
        PailPathLister lister = (PailPathLister) Utils.getObject(conf, PAIL_PATH_LISTER);
        if(lister==null) lister = new AllPailPathLister();
        return lister.getPaths(p);
    }

    public static PailSpec getDefaultCopy() {
        return new PailSpec(PailFormatFactory.SEQUENCE_FILE);
    }

    public static PailFormat create(PailSpec spec) {
        if(spec==null || spec.getName()==null) spec = getDefaultCopy();
        String format = spec.getName();
        Map<String, Object> args = spec.getArgs();
        if(args==null) args = new HashMap<String, Object>();
        if(format.equals(SEQUENCE_FILE)) {
            return new SequenceFileFormat(args);
        } else {
            try {
                return (PailFormat) Class.forName(format).newInstance();
            } catch(ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch(InstantiationException e) {
                throw new RuntimeException(e);
            } catch(IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
