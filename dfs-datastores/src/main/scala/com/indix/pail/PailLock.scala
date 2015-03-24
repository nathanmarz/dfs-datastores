package com.indix.pail

import com.indix.pail.PailLockType._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

/**
 * <b>Motivation</b> - When I have some jobs that are reading out of a pail location, consolidator runs and moves the
 * files around which causes the job to fail.
 *
 * <p />
 *
 * <b>Summary of fix</b> - Multiple READ locks can co-exist, but not more than 1 WRITE lock should exist on any pail root.
 *
 * <p />
 *
 * <b>Fix</b>
 *
 * <ol>
 * <li> When a Job A starts reading from a pail location, it touches a <em>.LOCK_READ_&ltg;JOB_NAME&gt;</em> file on the pail root. </li>
 * <li> Consolidator when starting checks for any .LOCK_READ* locks on the pail root before starting. It writes a .LOCK_WRITE_Consolidator </li>
 * file and waits until all READ locks are removed.  </li>
 * <li> During this if another Job B starts reading of the same pail location, job B waits in a loop until all WRITE locks are removed. </li>
 * </ol>
 *
 * @param lockType            Type of Lock you want to create
 * @param component           Name of the component
 * @param inputDirectory      pail root location
 * @param lockRecheckInterval pooling interval while waiting on either READ / WRITE locks.
 */
class PailLock(lockType: PailLockType, component: String, inputDirectory: String, lockRecheckInterval: Int = 30000) {
  def fileSystem(path: String) = new Path(path).getFileSystem(new Configuration)
  val logger = LoggerFactory.getLogger(this.getClass)

  val pathToLockFile = inputDirectory + "/" + ".LOCK_" + lockType + "_" + component

  def createLockFile() = {
    logger.info("Acquiring " + lockType + " lock...")
    fileSystem(pathToLockFile).createNewFile(new Path(pathToLockFile))
    logger.info("Acquired " + lockType + " lock.")
  }

  def acquire() = {
    lockType match {
      case WRITE => acquireWriteLock()
      case READ => acquireReadLock()
      case _ => throw new RuntimeException("Unsupported lock type.")
    }
    autoReleaseOnExit()
  }

  def release() = {
    logger.info("Attempting to release " + lockType + " lock...")
    fileSystem(pathToLockFile).delete(new Path(pathToLockFile), false)
    logger.info(lockType + " lock was released.")
  }

  private def autoReleaseOnExit() = {
    fileSystem(pathToLockFile).deleteOnExit(new Path(pathToLockFile))
  }

  private def acquireWriteLock() = {
    waitUntilLockIsFree(".LOCK_")
    createLockFile()
  }

  private def acquireReadLock() = {
    waitUntilLockIsFree(".LOCK_WRITE_")
    createLockFile()
  }

  private def waitUntilLockIsFree(lockPrefix: String) = {
    if (fileSystem(inputDirectory).listStatus(new Path(inputDirectory)).exists(_.getPath.toString.contains(lockPrefix))) {
      logger.info("Unable to acquire " + lockType + " lock. Entering wait phase...")
    }

    while (fileSystem(inputDirectory).listStatus(new Path(inputDirectory)).exists(_.getPath.toString.contains(lockPrefix))) {
      sleep(lockRecheckInterval)
    }
  }

  private def sleep(interval: Int) = {
    Thread.sleep(interval)
  }
}

object PailLock extends App {
  if (args.size != 4) {
    println("Usage: <WRITE|READ> <COMPONENT_NAME> <INPUT_DIRECTORIES> <ACQUIRE|RELEASE>")
    System.exit(-1)
  }

  val directories = args(2).split(",")
  val lockType = args(0).toUpperCase match {
    case "READ" => PailLockType.READ
    case "WRITE" => PailLockType.WRITE
  }

  args(3) match {
    case "ACQUIRE" =>
      directories.foreach(directory => new PailLock(lockType, args(1), directory).acquire())
    case "RELEASE" =>
      directories.foreach(directory => new PailLock(lockType, args(1), directory).release())
    case _ =>
      new RuntimeException("Unsupported lock operation")
  }
}

object PailLockType {

  trait PailLockType

  case object WRITE extends PailLockType

  case object READ extends PailLockType

}
