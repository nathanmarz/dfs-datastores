package com.backtype.hadoop.pail;

import org.apache.hadoop.conf.Configuration;

public class CopyArgs {
    public Integer renameMode = null;
    public boolean copyMetadata = true;
    public boolean force = false;
    transient public Configuration conf = null;

    public CopyArgs(CopyArgs other) {
        this.renameMode = other.renameMode;
        this.copyMetadata = other.copyMetadata;
        this.force = other.force;
        this.conf = other.conf;
    }

    public CopyArgs() {

    }
}
