package com.backtype.cascading.tap;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import com.backtype.hadoop.datastores.VersionedStore;
import com.backtype.support.CascadingUtils;
import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.hadoop.Hfs;

public class VersionedTap extends Hfs {
  public static enum TapMode {SOURCE, SINK}

  public Long version = null;

  // a sane default for the number of versions of your data to keep around
  private int versionsToKeep = 3;

  // source-specific
  public TapMode mode;

  // sink-specific
  private String newVersionPath;

  public VersionedTap(String dir, Scheme<JobConf,RecordReader,OutputCollector,?,?> scheme, TapMode mode)
      throws IOException {
    super(scheme, dir);
    this.mode = mode;
  }


  public VersionedTap setVersion(long version) {
    this.version = version;
    return this;
  }

  /**
    * Sets the number of versions of your data to keep. Unneeded versions are cleaned up on creation
    * of a new one. Pass a negative number to keep all versions.
    */
  public VersionedTap setVersionsToKeep(int versionsToKeep) {
    this.versionsToKeep = versionsToKeep;
    return this;
  }

  public int getVersionsToKeep() {
    return this.versionsToKeep;
  }

  public String getOutputDirectory() {
    return getPath().toString();
  }

  public VersionedStore getStore(JobConf conf) throws IOException {
    return new VersionedStore(FileSystem.get(conf), getOutputDirectory());
  }

  public String getSourcePath(JobConf conf) {
    VersionedStore store;
    try {
      store = getStore(conf);
      return (version != null) ? store.versionPath(version) : store.mostRecentVersionPath();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String getSinkPath(JobConf conf) {
    try {
      VersionedStore store = getStore(conf);
      return version == null ? store.createVersion() : store.createVersion(version);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf) {
    super.sourceConfInit(process, conf);
    FileInputFormat.setInputPaths(conf, getSourcePath(conf));
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
    super.sinkConfInit(process, conf);

    if (newVersionPath == null)
      newVersionPath = getSinkPath(conf);

    FileOutputFormat.setOutputPath(conf, new Path(newVersionPath));
  }

  @Override
  public boolean resourceExists(JobConf jc) throws IOException {
    return getStore(jc).mostRecentVersion() != null;
  }

  @Override
  public boolean createResource(JobConf jc) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean deleteResource(JobConf jc) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getIdentifier() {
    String outDir = getOutputDirectory();
    String versionString = (version == null) ? "LATEST" : version.toString();
    return "manhattan"
           + ((mode == TapMode.SINK) ? "sink" : "source")
           + ":" + outDir + ":" + versionString;
  }

  @Override
  public boolean commitResource(JobConf conf) throws IOException {
    VersionedStore store = new VersionedStore(FileSystem.get(conf), getOutputDirectory());

    if (newVersionPath != null) {
      store.succeedVersion(newVersionPath);
      CascadingUtils.markSuccessfulOutputDir(new Path(newVersionPath), conf);
      newVersionPath = null;
      store.cleanup(getVersionsToKeep());
    }

    return true;
  }

  @Override
  public boolean rollbackResource(JobConf conf) throws IOException {
    if (newVersionPath != null) {
      getStore(conf).failVersion(newVersionPath);
      newVersionPath = null;
    }

    return true;
  }
}
