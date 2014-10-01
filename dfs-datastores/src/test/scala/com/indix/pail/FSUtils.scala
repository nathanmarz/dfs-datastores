package com.indix.pail

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}

import scala.io.Source
import scala.util.Try

trait FSUtils {
  def customConfig: Configuration

  val _fsDefaultName = {
    val config = customConfig
    Option(config.get("fs.defaultFS")).getOrElse(config.get("fs.default.name"))
  }

  @transient lazy val fileSystem = {
    val enchancedConfig = new Configuration(customConfig)
    enchancedConfig.set("fs.defaultFS", _fsDefaultName)

    FileSystem.get(enchancedConfig)
  }

  def pathExists(path: String) = {
    fileSystem.exists(new Path(path))
  }

  def findFiles(path: String) = {
    fileSystem.globStatus(new Path(path)).map(_.getPath.toString)
  }

  def delete(path: String, recursive: Boolean = false) = {
    fileSystem.delete(new Path(path), recursive)
  }

  def mkdir(path: String) = {
    fileSystem.mkdirs(new Path(path))
  }

  def findFiles(path: String, pathFilter: PathFilter) = {
    fileSystem.globStatus(new Path(path), pathFilter).map(_.getPath.toString)
  }

  def touchFile(path: String) = {
    fileSystem.createNewFile(new Path(path))
  }

  def readLocalFile(path: String) = {
    Source.fromFile(path).getLines().toSet
  }

  def readHDFSFile(path: String) = {
    val fs = Try(FileSystem.get(URI.create(path), customConfig)).getOrElse(fileSystem)
    Source.fromInputStream(fs.open(new Path(path))).getLines().toSet
  }

}
