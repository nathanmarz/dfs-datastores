package com.indix.commons

import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


trait FSUtils {
  def conf: Configuration

  private def fs(path: String): FileSystem = {
    fs(path, conf)
  }
  
  def fs(path: String, configuration: Configuration) = {
    new Path(path).getFileSystem(configuration)
  }

  def readLines(hdfsPath: String) = {
    val path = new Path(hdfsPath)
    io.Source.fromInputStream(fs(hdfsPath).open(path)).getLines()
  }

  def readLocalFile(path: String) = {
    io.Source.fromFile(new File(path)).getLines()
  }

  def exists(path: String) = {
    fs(path).exists(new Path(path))
  }

  def delete(hdfsPath: String, recursive: Boolean = false) = {
    fs(hdfsPath).delete(new Path(hdfsPath), recursive)
  }

  def deleteOnExit(hdfsPath: String, recursive: Boolean = false) = {
    fs(hdfsPath).deleteOnExit(new Path(hdfsPath))
  }

  def create(hdfsPath: String) = {
    fs(hdfsPath).create(new Path(hdfsPath))
  }

  def touch(hdfsPath: String) = {
    fs(hdfsPath).create(new Path(hdfsPath)).close()
  }

  def mkdir(hdfsPath: String) = {
    fs(hdfsPath).mkdirs(new Path(hdfsPath))
  }
}
