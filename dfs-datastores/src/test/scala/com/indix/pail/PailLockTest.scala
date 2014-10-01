package com.indix.pail

import com.indix.pail.PailLockType._
import org.apache.hadoop.conf.Configuration
import org.scalatest.Matchers.{be, convertToAnyShouldWrapper}
import org.scalatest.{BeforeAndAfterEach, FunSpec}

class PailLockTest extends FunSpec with FSUtils with BeforeAndAfterEach {
  def customConfig = {
    val config = new Configuration()
    config.set("fs.default.name", "file:///")
    config
  }

  val inputDir = "/tmp/pail_lock_test/"

  override def beforeEach() {
    delete(inputDir, true)
    mkdir(inputDir)
  }

  override def afterEach() {
    delete(inputDir, true)
  }

  describe("Acquire pail read lock") {
    it("should acquire read lock even when other read locks are present") {
      val existingLock = new PailLock(READ, "COMPONENT_1", inputDir, 100)
      existingLock.acquire()
      val newLock = new PailLock(READ, "COMPONENT_2", inputDir, 100)
      newLock.acquire()

      pathExists(existingLock.pathToLockFile) should be(true)
      pathExists(newLock.pathToLockFile) should be(true)
    }

    it("should acquire read lock when existing write lock is released") {
      val existingLock = new PailLock(WRITE, "CONSOLIDATE", inputDir, 100)
      existingLock.acquire()
      val newLock = new PailLock(READ, "COMPONENT_1", inputDir, 100)
      //blocks for lock
      val thread = new Thread(new Runnable() {
        override def run(): Unit = newLock.acquire()
      })

      Thread.sleep(500)
      existingLock.release()
      thread.join()

      pathExists(existingLock.pathToLockFile) should be(false)
      pathExists(newLock.pathToLockFile) should be(false)
    }

    it("should not acquire read lock when a write lock is present") {
      val existingWriteLock = new PailLock(WRITE, "CONSOLIDATE", inputDir, 100)
      existingWriteLock.acquire()

      val newLock = new PailLock(READ, "COMPONENT_2", inputDir, 100)
      //blocks for lock
      val thread = new Thread(new Runnable() {
        override def run(): Unit = newLock.acquire()
      })
      thread.join(500)
      pathExists(existingWriteLock.pathToLockFile) should be(true)
      pathExists(newLock.pathToLockFile) should be(false)

    }
  }

  describe("Acquire pail write lock") {
    it("should not acquire write lock when other locks are present") {
      val existingLock = new PailLock(READ, "COMPONENT_1", inputDir, 100)
      existingLock.acquire()
      val newLock = new PailLock(WRITE, "CONSOLIDATE", inputDir, 100)
      //blocks for lock
      val thread = new Thread(new Runnable() {
        override def run(): Unit = newLock.acquire()
      })
      thread.join(500)

      pathExists(existingLock.pathToLockFile) should be(true)
      pathExists(newLock.pathToLockFile) should be(false)
    }

    it("should acquire write lock when existing read lock is released") {
      val existingLock = new PailLock(READ, "COMPONENT_1", inputDir, 100)
      existingLock.acquire()
      val newLock = new PailLock(WRITE, "CONSOLIDATE", inputDir, 100)
      //blocks for lock
      val thread = new Thread(new Runnable() {
        override def run(): Unit = newLock.acquire()
      })

      Thread.sleep(500)
      existingLock.release()
      thread.join()

      pathExists(existingLock.pathToLockFile) should be(false)
      pathExists(newLock.pathToLockFile) should be(false)
    }

    it("should not acquire write lock when any lock is present") {
      val existingReadLock = new PailLock(READ, "COMPONENT_2", inputDir, 100)
      existingReadLock.acquire()

      val newLock = new PailLock(WRITE, "CONSOLIDATE", inputDir, 100)
      //blocks for lock
      val thread = new Thread(new Runnable() {
        override def run(): Unit = newLock.acquire()
      })
      thread.join(500)
      pathExists(existingReadLock.pathToLockFile) should be(true)
      pathExists(newLock.pathToLockFile) should be(false)

    }
  }
  describe("Release pail locks") {
    it("should release read lock") {
      val readLock = new PailLock(READ, "COMPONENT_2", inputDir, 100)
      readLock.acquire()
      pathExists(readLock.pathToLockFile) should be(true)
      readLock.release()
      pathExists(readLock.pathToLockFile) should be(false)
    }

    it("should release write lock") {
      val writeLock = new PailLock(WRITE, "COMPONENT_2", inputDir, 100)
      writeLock.acquire()
      pathExists(writeLock.pathToLockFile) should be(true)
      writeLock.release()
      pathExists(writeLock.pathToLockFile) should be(false)
    }
  }

}