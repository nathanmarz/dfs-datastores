package com.indix.pail

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterEach, FunSpec}

class PailConsolidateTest extends FunSpec with FSUtils with ShouldMatchers with BeforeAndAfterEach  {

  def customConfig: Configuration = new Configuration()
  customConfig.set("fs.default.name", "file:///")

  val inputDir = "/tmp/pail_consolidate_test/"

  override def beforeEach() {
    delete(inputDir, true)
    mkdir(inputDir)
  }

  describe("Pail consolidation locking mechanism") {
    val inputDir= "/tmp/pail_consolidate_test/"

    it("should wait indefinitely if a lock could not be acquired") {
      val readPailLock = new PailLock(PailLockType.READ, "PRODUCT_REFRESH", inputDir)
      readPailLock.acquire()

      val thread = new Thread(new Runnable() {
        val pailConsolidate = new PailConsolidate(inputDir, inputDir, "MANUAL") {
          override val writeLock = new PailLock(PailLockType.WRITE, "CONSOLIDATE", inputDir, 200)
          override def consolidateUsingPail(fileSystem: FileSystem, consolidationDir: String) = { }
        }

        override def run() {
          pailConsolidate.run()
        }
      })

      thread.start()
      thread.join(500)

      pathExists(inputDir + "/.LOCK_WRITE_CONSOLIDATE") should be(false)
      pathExists(readPailLock.pathToLockFile) should be(true)
    }

    it("should run consolidation when no lock is present") {
      val thread = new Thread(new Runnable() {
        val pailConsolidate = new PailConsolidate(inputDir, inputDir, "MANUAL") {
          override val writeLock = new PailLock(PailLockType.WRITE, "CONSOLIDATE", inputDir, 200)
          override def consolidateUsingPail(fileSystem: FileSystem, consolidationDir: String): Unit = {
            pathExists(inputDir + "/.LOCK_WRITE_CONSOLIDATE") should be(true)
          }
        }

        override def run() {
          pailConsolidate.run()
        }
      })

      thread.start()
      thread.join()

      pathExists(inputDir + "/.LOCK_WRITE_CONSOLIDATE") should be(false)
    }

    it("should delete the consolidate lock file even if something goes wrong somewhere") {
      val thread = new Thread(new Runnable() {
        val pailConsolidate = new PailConsolidate(inputDir, inputDir, "MANUAL") {
          override val writeLock = new PailLock(PailLockType.WRITE, "CONSOLIDATE", inputDir, 200)
          override def consolidateUsingPail(fileSystem: FileSystem, consolidationDir: String): Unit = {
            pathExists(inputDir + "/.LOCK_WRITE_CONSOLIDATE") should be(true)
            throw new RuntimeException("Something terrible happened.")
          }
        }

        override def run() {
          pailConsolidate.run()
        }
      })

      thread.start()
      thread.join()

      pathExists(inputDir + "/.LOCK_WRITE_CONSOLIDATE") should be(false)
    }
  }

}
