package com.indix.commons

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.commons.lang3.StringUtils
import java.net.URI
import org.apache.hadoop.io.SequenceFile
import java.io.{ObjectOutputStream, InputStream, FileWriter}
import org.apache.commons.io.IOUtils


trait FS {
  def fs = {
    val configuration = new Configuration()
    FileSystem.get(configuration)
  }

  def changeFSUri(hdfsHost: String) = {
    val conf = new Configuration()
    conf.set("fs.default.name", hdfsHost)
    conf.set("fs.defaultFS", hdfsHost)
    fs.setConf(conf)
    fs.initialize(new URI(hdfsHost), conf)
  }

  def exists(path: String): Boolean = {
    val fs = new Path(path).getFileSystem(new Configuration)
    fs.exists(new Path(path))
  }

  def exists(file: String, hdfsHost: String) = {
    val oldFsName = fs.getUri.toString
    hdfsHost match {
      case x : String if (StringUtils.isNotEmpty(hdfsHost)) => changeFSUri(hdfsHost)
      case _ =>
    }
    println("FS URI => "+fs.getUri)
    val isPresent = fs.exists(new Path(file))
    changeFSUri(oldFsName)
    isPresent
  }

  def pathShouldExist(path: String, conf: Configuration) = new Path(path).getFileSystem(conf).exists(new Path(path))

  def deleteFileIfExists(file: String, hdfsHost: String = "") = {
    val oldFsName = fs.getUri.toString
    hdfsHost match {
      case x : String if (StringUtils.isNotEmpty(hdfsHost)) => changeFSUri(hdfsHost)
      case _ =>
    }
    println("FS URI => "+fs.getUri)
    val isPresent = fs.exists(new Path(file))
    fs.delete(new Path(file), false)
    changeFSUri(oldFsName)
    isPresent
  }

  def getLines(file: String) = {
    val path = new Path(file)
    io.Source.fromInputStream(path.getFileSystem(new Configuration).open(path)).getLines()
  }

  def getLinesUsingIOUtils(file: String) = {
    IOUtils.readLines(fs.open(new Path(file)))
  }

  def getInputStream(file: String) = {
    fs.open(new Path(file))
  }

  def getLinesLocal(file: String) = {
    io.Source.fromFile(file).getLines()
  }

  def getLinesLocal(stream: InputStream) = {
    io.Source.fromInputStream(stream).getLines()
  }

  def writer(file: String) = {
    fs.create(new Path(file))
  }

  def write(file: String, content: Iterator[String]) = {
    val f = fs.create(new Path(file))
    content.foreach(f.writeBytes _)
    f.close
  }

  def writeJavaObject(file:String, obj: Object) = {
    val f = fs.create(new Path(file))
    val oos = new ObjectOutputStream(f)
    oos.writeObject(obj)
    oos.close()
  }

  def writeLocal(file: String, content: Iterator[String]) = {
    val f = new FileWriter(file)
    content.foreach(a => f.write(a))
    f.close
  }

  def seqFileReader[K,V](filePath: String, keyClass: Class[K], valueClass: Class[V], fsName: String = "file:///") = {
    val conf = new Configuration()
    conf.set("fs.default.name", fsName)
    val fs = FileSystem.get(conf)
    new SequenceFile.Reader(fs, new Path(filePath), conf)
  }
}
