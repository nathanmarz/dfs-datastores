package com.indix.commons

import java.io._
import scala.io.Source
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.lang3.StringUtils


trait FileHelper extends Serializable {

  def readAllLines(fileName: String): Iterator[String] = {
    io.Source.fromFile(fileName).getLines()
  }

  def writeLines(fileName: String, data: TraversableOnce[String]) {
    val writer = new PrintWriter(fileName)
    data.foreach(writer.println)
    writer.close()
  }

  def writeBytes(fileName: String, data: List[Array[Byte]]) {
    val writer = new FileOutputStream(fileName)
    data.foreach(writer.write)
    writer.close()
  }

  def appendLines(fileName: String, data: TraversableOnce[String]) {
    if(StringUtils.isNotEmpty(fileName)) {
      val writer = new PrintWriter(new FileWriter(fileName, true))
      data.foreach(writer.println)
      writer.close()
    }
  }

  def writeToHDFS(fileName: String, conf: Configuration, data: TraversableOnce[String]) {
    val bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream(fileName, conf)))
    data.foreach(x => bufferedWriter.write(x+"\n"))
    bufferedWriter.close()
  }

  def hdfsWriter(fileName: String, conf: Configuration) = {
    new BufferedWriter(new OutputStreamWriter(outputStream(fileName, conf)))
  }

  def writeBinToHDFS(fileName: String, conf: Configuration, data: TraversableOnce[Array[Byte]]) {
    val writer = outputStream(fileName, conf)
    data.foreach(writer.write)
    writer.close()
  }

  def readFromHDFS(fileName: String, conf : Configuration) : Iterator[String] = {
    val fs = inputStream(fileName, conf)
    val source = Source.fromInputStream(fs)
    source.getLines()
  }

  def hdfsCopy(inFile: String, outFile: String, conf: Configuration) = {
    val fs = FileSystem.get(conf)
    FileUtil.copy(fs, new Path(inFile), fs, new Path(outFile), false, conf)
  }

  def outputStream(fileName: String, conf: Configuration): FSDataOutputStream = {
    val path = new Path(fileName)
    FileSystem.get(conf).create(path)
  }

  def inputStream(fileName: String, conf: Configuration): FSDataInputStream = {
    val path = new Path(fileName)
    FileSystem.get(conf).open(path)
  }

  def moveFromLocal(localFilePath: String, pathOnHDFS: String, conf : Configuration) = {
    val fs = FileSystem.get(conf)
    fs.moveFromLocalFile(new Path(localFilePath), new Path(pathOnHDFS))
  }

  def copyFromLocal(localFilePath: String, pathOnHDFS: String, conf : Configuration) = {
    val fs = FileSystem.get(conf)
    fs.copyFromLocalFile(new Path(localFilePath), new Path(pathOnHDFS))
  }

  def overwriteFromLocal(localFilePath: String, pathOnHDFS: String, conf : Configuration) = {
    val fs = FileSystem.get(conf)
    if(new File(localFilePath).exists) fs.copyFromLocalFile(false, true, new Path(localFilePath), new Path(pathOnHDFS))
  }

  def mergeFiles(file1: String, file2: String, merged: String) {
    val fw = new FileWriter(merged, false)
    Source.fromFile(file1).getLines().foreach(line => fw.write(line+"\n")) ;
    Source.fromFile(file2).getLines().foreach(line => fw.write(line+"\n")) ;
    fw.close()
  }

  def deleteHDFSFile(filePath: String, conf: Configuration) {
    FileSystem.get(conf).delete(new Path(filePath), true)
  }

  def createDir(filePath: String, conf: Configuration) {
    FileSystem.get(conf).mkdirs(new Path(filePath))
  }

  def seqFileWriter[K,V](filePath: String, keyClass: Class[K], valueClass: Class[V], fsName: String = "file:///", buffer: Int = 4096) = {
    val conf = new Configuration()
    conf.set("fs.default.name", fsName)
    conf.setInt("io.file.buffer.size", buffer)
    conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec")
    val fs = FileSystem.get(conf)
    SequenceFile.createWriter(fs, conf, new Path(filePath), keyClass, valueClass)
  }

  def seqFileReader[K,V](filePath: String, keyClass: Class[K], valueClass: Class[V], fsName: String = "file:///", buffer: Int = 4096) = {
    val conf = new Configuration()
    conf.set("fs.default.name", fsName)
    conf.setInt("io.file.buffer.size", buffer)
    val fs = FileSystem.get(conf)
    new SequenceFile.Reader(fs, new Path(filePath), conf)
  }

  def sortSeqFile[K <: WritableComparable[_], V](inFilePath: String, outFilePath: String, keyClass: Class[K], valueClass: Class[V],fsName: String = "file:///") = {
    val conf = new Configuration()
    conf.set("fs.default.name", fsName)
    val fs = FileSystem.get(conf)
    val sorter = new SequenceFile.Sorter(fs, keyClass, valueClass, conf)
    sorter.sort(new Path(inFilePath), new Path(outFilePath))
  }

  def mapFileWriter[K <: WritableComparable[_],V](filePath: String, keyClass: Class[K], valueClass: Class[V],
                                                  indexInterval: Int = 128, fsName: String = "file:///", buffer: Int = 4096) = {
    val conf = new Configuration()
    conf.set("fs.default.name", fsName)
    conf.setInt("io.file.buffer.size", buffer)
    conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec")
    val fs = FileSystem.get(conf)
    val writer = new MapFile.Writer(conf, fs, filePath, keyClass, valueClass)
    writer.setIndexInterval(indexInterval)
    writer
  }

  def mapFileReader[K <: WritableComparable[_],V](filePath: String, fsName: String = "file:///", buffer: Int = 4096) = {
    val conf = new Configuration()
    conf.set("fs.default.name", fsName)
    conf.setInt("io.file.buffer.size", buffer)
    val fs = FileSystem.get(conf)
    new MapFile.Reader(fs, filePath, conf)
  }

  def getMerge(dirToMerge: String, outputFilePath: String, conf: Configuration, fsName: String = "file:///") = {
    val conf = new Configuration()
    conf.set("fs.default.name", fsName)
    val srcPath = new Path(dirToMerge)
    val fs = srcPath.getFileSystem(conf)
    val srcs = FileUtil.stat2Paths(fs.globStatus(srcPath))
    val dst = new Path(outputFilePath)
    srcs.foreach(src => {
      FileUtil.copyMerge(fs, src, fs, dst, false, conf, null)
    })
  }

  def exists(filePath: String, conf: Configuration, fsName: String = "file:///") : Boolean = {
    val conf = new Configuration()
    conf.set("fs.default.name", fsName)
    val fs = FileSystem.get(conf)
    fs.exists(new Path(filePath))
  }
}
