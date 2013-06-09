package com.backtype.hadoop.pail;

import org.apache.hadoop.conf.Configuration;

public class CopyArgs {
    public Integer renameMode = null;
    public boolean copyMetadata = true;
    public boolean force = false;
    public Configuration configuration = new Configuration();

    public CopyArgs(CopyArgs other) {
        this.renameMode = other.renameMode;
        this.copyMetadata = other.copyMetadata;
        this.force = other.force;
        this.configuration = other.configuration;
    }

    public CopyArgs() {

    }
}
