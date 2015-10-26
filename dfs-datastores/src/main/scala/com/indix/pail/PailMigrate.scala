package com.indix.pail

import java.io.IOException

import com.backtype.hadoop.pail.{PailOutputFormat, PailStructure}
import com.backtype.hadoop.pail.SequenceFileFormat.SequenceFilePailInputFormat
import com.backtype.support.Utils
import com.indix.pail.PailMigrate.PailMigrateMapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.Tool
import org.apache.log4j.Logger

class PailMigrate extends Tool {
  val logger = Logger.getLogger(this.getClass)

  /*
  * Takes an input pail location, an output pail location and a output pail spec
  * - Setup job to process input pail location
  * - Deserialize record
  * - Write to output location using the output spec
  * - If output dir already exists, just append to it, instead of writing to temp and absorbing
  * - Finally, clear out all processed files (disable source removal based on configuration).
  *
  * InputFormat - PailInputFormat
  * OutputFormat - PailOutputFormat - needs a PailSpec
  * */

  override def run(args: Array[String]): Int = {
    if (args.length != 3) {
      println(s"Usage: hadoop job <JAR> ${this.getClass.getCanonicalName} <input_dir> <output_dir> <pail_spec_class_fqcn> [<keep_source_files>]")
      println("input_dir - Input pail dir from which records need to be read and copied(or moved)")
      println("output_dir - Destination pail dir to which records need to be written to")
      println("pail_spec_class_fqcn - Fully qualified class name of output PailStructure. This will be used to decide the partitioning scheme")
      println("keep_source_files - set this to true to keep the source files. This is optional, Defaults to false, which means, source files will be removed if the job succeeds")
      System.exit(1)
    }

    /*Input Pail location to pick up and migrate
    * Input paths will be deleted unless `keepSourceFiles` flag is set
    * */
    val inputDir = args(0)

    /*
    * Output location to write the records
    * The output location will be (vertically) partitioned based on the `specClass` parameter
    * */
    val outputDir = args(1)

    /*
    * FQCN of the PailStructure class. Note that, the class that's passed here should be available in the classpath
    * This is used to write individual records into appropriate vertical partitions in the output location
    * */
    val specClass = args(2)


    val keepSourceFiles = Option(args(3)).exists(_ equals "true")

    val pailStructure = Class.forName(specClass).newInstance().asInstanceOf[PailStructure]

    val jobConf = new JobConf(getConf)
    jobConf.setJobName("Pail Migration job (from one scheme to another)")
    
    jobConf.setInputFormat(classOf[SequenceFilePailInputFormat])
    FileInputFormat.addInputPath(jobConf, new Path(inputDir))

    jobConf.setOutputFormat(classOf[PailOutputFormat])
    FileOutputFormat.setOutputPath(jobConf, new Path(outputDir))

    Utils.setObject(jobConf, PailMigrate.OUTPUT_STRUCTURE, pailStructure)

    jobConf.setMapperClass(classOf[PailMigrateMapper])

    jobConf.setNumReduceTasks(0)
    jobConf.setJarByClass(this.getClass)

    val job = new JobClient(jobConf).submitJob(jobConf)

    logger.info(s"Pail Migrate triggered for $inputDir")
    logger.info("Submitted job "+job.getID)
    
    while(!job.isComplete){
      Thread.sleep(30*1000)
    }

    if(!job.isSuccessful) throw new IOException("Pail Migrate failed")

    val path: Path = new Path(inputDir)
    val fs = path.getFileSystem(getConf)

    if(!keepSourceFiles) {
      logger.info(s"Deleting path ${inputDir}")
      val deleteStatus = fs.delete(path, true)

      if(!deleteStatus)
        logger.warn(s"Deleting ${inputDir} failed. \n *** Please delete the source manually ***")
      else
        logger.info(s"Deleting ${inputDir} completed successfully.")
    }

    0 // return success, failures throw an exception anyway!
  }
  
  override def getConf: Configuration = getConf

  override def setConf(configuration: Configuration): Unit = super.setConf(configuration)
}

object PailMigrate {
  val OUTPUT_STRUCTURE = "pail.migrate.output.structure"

  class PailMigrateMapper extends Mapper[Text, BytesWritable, Text, BytesWritable] {
    var outputPailStructure: PailStructure = null

    override def map(key: Text, value: BytesWritable, outputCollector: OutputCollector[Text, BytesWritable], reporter: Reporter): Unit = {
      val record = outputPailStructure.deserialize(value.getBytes)
      val key = new Text(Utils.join(outputPailStructure.getTarget(record), "/"))
      outputCollector.collect(key, value)
    }

    override def close(): Unit = {}

    override def configure(jobConf: JobConf): Unit = {
      outputPailStructure = Utils.getObject(jobConf, OUTPUT_STRUCTURE).asInstanceOf[PailStructure]
    }
  }

}


