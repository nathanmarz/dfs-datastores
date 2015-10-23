package com.indix.pail

import java.io.File
import java.util

import com.backtype.hadoop.{BalancedDistcp, PathLister, RenameMode}
import com.indix.models.pail.DateHelper
import com.indix.models.utils.ModelUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.scala_tools.time.StaticDateTime._

import scala.collection.JavaConversions._
import scala.io.Source

class DatewiseHostPathFilter(sitelist: List[(String, Int)], rootDir: String) extends PathLister {
  val today = now
  val currentHour = now.getHourOfDay

  /*
    Map of
      directory root -> (isWeeklyArchive, isSiteIndex)

    TODO: Remove the isSiteIndex after all the pail type migrations
   */
  val weeklyArchive = Map(
    "/crawler/master" -> (true, true),
    "/products/new" -> (false, false),
    "/products/master" -> (true, false),
    "/prices/new" -> (false, false),
    "/prices/master" -> (true, false)
  )

  def getPathPatterns = {
    val paths = sitelist.flatMap{ siteDaysTuple =>
      val (site, numDays) = siteDaysTuple
      weeklyArchive(rootDir) match {
        case (false, false) => {
          val timestamps = (1 to numDays).map(p => today.minusDays(p).toString("YYYYMMdd")).toSet
          val olderDaysDataPaths = timestamps.map(t => "%s/%s/*/%s".format(rootDir, t, site))
          val todaysDataPaths = (0 to currentHour-1).map(hr => "%s/%s/%02d/%s".format(rootDir, today.toString("YYYYMMdd"), hr, site)).toSet
          olderDaysDataPaths ++ todaysDataPaths
        }
        case (true, false) => {
          val timestamps = (0 to numDays).map(p => DateHelper.weekInterval(today.minusDays(p))).toSet
          timestamps.map(t => "%s/%s/%s".format(rootDir, t, site))
        }

        case (true, true) => {
          val timestamps = (0 to numDays).map(p => DateHelper.weekInterval(today.minusDays(p))).toSet
          timestamps.map(t => "%s/%s/%s".format(rootDir, t, ModelUtils.domainIndex(siteNameToQualifiedUri(site))))
        }
      }
    }.toSet
    paths
  }

  // gets file list from HDFS.
  // all paths returned by this method should be valid HDFS paths
  def fileList(pattern: String, fs: FileSystem, pathFilter: PathFilter): Array[String] = {
    val paths = {
      if(pattern.contains("/*/")){
        fs.globStatus(new Path(pattern), pathFilter)
      }else{
        val path: Path = new Path(pattern)
        if (fs.exists(path))
          fs.listStatus(path, pathFilter)
        else
          Array[FileStatus]()
      }
    }
    paths.map(_.getPath.toUri.getPath).distinct
  }


  def getFiles(fs: FileSystem, baseDir: String): util.List[Path] = {
    val pathFilter = new PathFilter {
      def accept(path: Path): Boolean = {
        !path.getName.endsWith(".pailfiletmp")
      }
    }

    val filePatterns = getPathPatterns
    val paths = filePatterns.flatMap{ pattern => fileList(pattern, fs, pathFilter)}.map(p => new Path(p)).toList
    // select 'files' under 'paths'
    val files = paths.map(p => fs.listFiles(p, true)).flatMap{path =>
      val list = new util.ArrayList[Path]()
      while(path.hasNext){
        val p = path.next
        if (p.isFile)
          list.add(p.getPath)
      }
      list
    }

    println(files.size+" files being copied from basedir: "+baseDir)
    files
  }

  private def siteNameToQualifiedUri(site: String) = {
    if(site.startsWith("http")) site
    else "http://%s/".format(site)
  }
}

object GenerateFileListForPailCopy {
  def readSitesFile(file: String) = Source.fromFile(file).getLines()

  def main(args: Array[String]) {
    args.length match {
      case x if x < 3 => {
        println("Usage: GenerateFileListForPailCopy input_file ouput_location source_hdfs_host")
        System.exit(1)
      }
      case _ => {}
    }

    val sitesFile = args(0)
    val outputRootDir = args(1)
    val sourceHdfsHost = args(2)

    val lines = readSitesFile(sitesFile)
    val input = lines.map(a => (a.split(",")(0), a.split(",")(1))).map(pair => (pair._1, pair._2.toInt)).toList


    val rootDirs = {
      args.length match {
        case _ => List("/crawler/master", "/products/new", "/products/master", "/prices/new", "/prices/master")
      }
    }

    rootDirs.foreach{ rootDir  =>
      println("Running balanced distcp for %s to output location %s".format(rootDir, outputRootDir))
      val baseDirUri = "%s%s".format(sourceHdfsHost, File.separator) + rootDir
      BalancedDistcp.distcp(baseDirUri,
        outputRootDir+File.separator+rootDir,
        RenameMode.NO_RENAME,
        new DatewiseHostPathFilter(input, rootDir),
        ".pailfile",
        new Configuration()
      )
    }
  }
}