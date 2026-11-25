/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*** spark-rapids-shim-json-lines
{"spark":["401","402","411"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.delta.rapids

import java.io.{File, FilterOutputStream, IOException}
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, Executors, ExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import com.nvidia.spark.rapids.{RapidsConf, SparkQueryCompareTestSuite}
import org.apache.hadoop.fs.{FSDataOutputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import org.apache.spark.sql.delta.commands.{DeletionVectorData, DeletionVectorResult,
  DeletionVectorWriter}
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat,
  StoredBitmap}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore

/** A local file system that records the threads creating files. */
class ThreadRecordingFs extends RawLocalFileSystem {
  override def create(f: Path, overwrite: Boolean, bufferSize: Int, replication: Short,
      blockSize: Long, progress: Progressable): FSDataOutputStream = {
    ThreadRecordingFs.threads.add(Thread.currentThread().getName)
    super.create(f, overwrite, bufferSize, replication, blockSize, progress)
  }

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
      replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = {
    ThreadRecordingFs.threads.add(Thread.currentThread().getName)
    super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress)
  }
}

object ThreadRecordingFs {
  val threads = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
}

/** A local file system whose files fail on close, like an upload failing on an object store. */
class FailOnCloseFs extends RawLocalFileSystem {
  private def failOnClose(stream: FSDataOutputStream): FSDataOutputStream =
    new FSDataOutputStream(new FilterOutputStream(stream) {
      override def write(b: Array[Byte], off: Int, len: Int): Unit = stream.write(b, off, len)
      override def close(): Unit = {
        stream.close()
        throw new IOException("upload failed")
      }
    }, null)

  override def create(f: Path, overwrite: Boolean, bufferSize: Int, replication: Short,
      blockSize: Long, progress: Progressable): FSDataOutputStream =
    failOnClose(super.create(f, overwrite, bufferSize, replication, blockSize, progress))

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
      replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream =
    failOnClose(
      super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress))
}

class GpuDeletionVectorWriterSuite extends SparkQueryCompareTestSuite {
  private def withPool[T](size: Int)(f: ExecutorService => T): T = {
    val pool = Executors.newFixedThreadPool(size)
    try f(pool) finally pool.shutdownNow()
  }

  test("no bins produce no results") {
    assert(GpuDeletionVectorWriter.writeBins(Iterator.empty[Seq[Int]], (b: Seq[Int]) => b,
      maxInFlight = 4).isEmpty)
  }

  test("a single bin is written on the calling thread") {
    val caller = Thread.currentThread()
    val writers = new java.util.concurrent.ConcurrentLinkedQueue[Thread]()
    val results = GpuDeletionVectorWriter.writeBins(
      Iterator(Seq(1, 2)),
      (bin: Seq[Int]) => { writers.add(Thread.currentThread()); bin },
      maxInFlight = 4,
      pool = fail("a single bin must not use the pool")).toSeq
    assert(results == Seq(1, 2))
    assert(writers.toArray.toSeq == Seq(caller))
  }

  test("a limit of one writes every bin on the calling thread, one at a time and lazily") {
    val caller = Thread.currentThread()
    val writers = new java.util.concurrent.ConcurrentLinkedQueue[Thread]()
    val results = GpuDeletionVectorWriter.writeBins(
      (0 until 3).iterator.map(i => Seq(i)),
      (bin: Seq[Int]) => { writers.add(Thread.currentThread()); bin },
      maxInFlight = 1,
      pool = fail("a limit of one must not use the pool"))
    assert(writers.isEmpty, "bins were written before they were requested")
    assert(results.next() == 0)
    assert(writers.size == 1, "more than the requested bin was written")
    assert(results.toSeq == Seq(1, 2))
    assert(writers.toArray.toSeq == Seq(caller, caller, caller))
  }

  test("bins are written concurrently and results keep the bin order") {
    withPool(4) { pool =>
      val allStarted = new CountDownLatch(4)
      val results = GpuDeletionVectorWriter.writeBins(
        (0 until 4).iterator.map(i => Seq(i, i + 10)),
        (bin: Seq[Int]) => {
          // Only completes if all four bins are in flight together.
          allStarted.countDown()
          assert(allStarted.await(30, TimeUnit.SECONDS), "bins were written serially")
          bin
        },
        maxInFlight = 4,
        pool = pool).toSeq
      assert(results == Seq(0, 10, 1, 11, 2, 12, 3, 13))
    }
  }

  test("the number of bins in flight is bounded") {
    withPool(8) { pool =>
      val inFlight = new AtomicInteger(0)
      val maxSeen = new AtomicInteger(0)
      val limitReached = new CountDownLatch(3)
      val results = GpuDeletionVectorWriter.writeBins(
        (0 until 20).iterator.map(i => Seq(i)),
        (bin: Seq[Int]) => {
          val now = inFlight.incrementAndGet()
          maxSeen.updateAndGet(m => math.max(m, now))
          // The first bins only finish once the limit is reached, so it is reached on any schedule.
          limitReached.countDown()
          assert(limitReached.await(30, TimeUnit.SECONDS), "the bins did not overlap")
          inFlight.decrementAndGet()
          bin
        },
        pool = pool,
        maxInFlight = 3).toSeq
      assert(results == (0 until 20))
      assert(maxSeen.get() == 3)
    }
  }

  test("a failed bin fails the task, starts no new files and waits for running ones") {
    withPool(4) { pool =>
      val started = new AtomicInteger(0)
      val running = new AtomicInteger(0)
      val completed = new AtomicInteger(0)
      val failed = new CountDownLatch(1)
      val allStarted = new CountDownLatch(4)
      val ex = intercept[IllegalStateException] {
        GpuDeletionVectorWriter.writeBins(
          (0 until 100).iterator.map(i => Seq(i)),
          (bin: Seq[Int]) => {
            started.incrementAndGet()
            running.incrementAndGet()
            try {
              allStarted.countDown()
              if (bin.head == 1) {
                // Fail only once the other bins are running, as later ones would be skipped.
                assert(allStarted.await(30, TimeUnit.SECONDS))
                failed.countDown()
                throw new IllegalStateException("boom")
              }
              // The other bins are running when the failure happens.
              assert(failed.await(30, TimeUnit.SECONDS))
              completed.incrementAndGet()
              bin
            } finally {
              running.decrementAndGet()
            }
          },
          pool = pool,
          maxInFlight = 4).toSeq
      }
      assert(ex.getMessage == "boom")
      // No writer may still be running once the failure is reported.
      assert(running.get() == 0)
      // Bins 0, 2 and 3 were running; a later bin may also start before the abort is seen.
      assert(completed.get() >= 3, "running bins were not waited for")
      assert(started.get() < 100)
    }
  }

  test("an input iterator that fails mid-stream fails the task after running bins finish") {
    withPool(4) { pool =>
      val running = new AtomicInteger(0)
      val completed = new AtomicInteger(0)
      val inputFailed = new CountDownLatch(1)
      val allStarted = new CountDownLatch(4)
      val bins = (0 until 5).iterator.map { i =>
        if (i == 4) {
          // Fail only once all bins are running, as later ones would be skipped.
          assert(allStarted.await(30, TimeUnit.SECONDS))
          inputFailed.countDown()
          throw new IllegalStateException("bad input")
        }
        Seq(i)
      }
      val ex = intercept[IllegalStateException] {
        GpuDeletionVectorWriter.writeBins(
          bins,
          (bin: Seq[Int]) => {
            running.incrementAndGet()
            try {
              allStarted.countDown()
              // The bins are still running when the input fails.
              assert(inputFailed.await(30, TimeUnit.SECONDS))
              completed.incrementAndGet()
              bin
            } finally {
              running.decrementAndGet()
            }
          },
          pool = pool,
          maxInFlight = 4).toSeq
      }
      assert(ex.getMessage == "bad input")
      assert(running.get() == 0)
      assert(completed.get() == 4, "running bins were not waited for")
    }
  }

  test("failures of other bins are kept as suppressed exceptions") {
    withPool(2) { pool =>
      val bothRunning = new CountDownLatch(2)
      val ex = intercept[IllegalStateException] {
        GpuDeletionVectorWriter.writeBins(
          (0 until 2).iterator.map(i => Seq(i)),
          (bin: Seq[Int]) => {
            bothRunning.countDown()
            assert(bothRunning.await(30, TimeUnit.SECONDS))
            throw new IllegalStateException(s"boom ${bin.head}")
          },
          pool = pool,
          maxInFlight = 2).toSeq
      }
      assert(ex.getMessage == "boom 0")
      assert(ex.getSuppressed.map(_.getMessage).toSeq == Seq("boom 1"))
    }
  }

  test("an interrupted task waits for running bins and keeps its interrupt flag") {
    withPool(4) { pool =>
      val running = new AtomicInteger(0)
      val inWriters = new CountDownLatch(2)
      val release = new CountDownLatch(1)
      var runningOnExit = -1
      var flagOnExit = false
      val task = new Thread(() => {
        try {
          GpuDeletionVectorWriter.writeBins(
            (0 until 2).iterator.map(i => Seq(i)),
            (bin: Seq[Int]) => {
              running.incrementAndGet()
              inWriters.countDown()
              try {
                assert(release.await(30, TimeUnit.SECONDS))
                bin
              } finally {
                running.decrementAndGet()
              }
            },
            maxInFlight = 2,
            pool = pool).toSeq
        } catch {
          case _: InterruptedException => // the interrupt of the wait is reported as is
        }
        runningOnExit = running.get()
        flagOnExit = Thread.currentThread().isInterrupted
      })
      task.start()
      assert(inWriters.await(30, TimeUnit.SECONDS))
      task.interrupt()
      // The writers are blocked, so the task must still be waiting for them after the interrupt.
      task.join(500)
      assert(task.isAlive, "the task exited without waiting for the running writers")
      release.countDown()
      task.join(30000)
      assert(runningOnExit == 0, "writers were still running when the task exited")
      assert(flagOnExit, "the interrupt flag was lost")
    }
  }

  private def newBitmap(values: Long*): (Array[Byte], Long) = {
    val bitmap = new RoaringBitmapArray()
    values.foreach(bitmap.add)
    (bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable), bitmap.cardinality)
  }

  private def dvFiles(table: File): Seq[File] =
    table.listFiles().filter(_.getName.startsWith("deletion_vector_")).toSeq

  private def allDvFiles(table: File): Seq[File] =
    org.apache.commons.io.FileUtils.listFiles(table, null, true).asScala.toSeq
      .filter(_.getName.startsWith("deletion_vector_"))

  test("the DV mapper persists packed files that Delta can read back") {
    val table = Files.createTempDirectory("gpu-dv-writer").toFile
    try {
      withCpuSparkSession { spark =>
        // A tiny packing target forces one file per bitmap.
        spark.conf.set(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE.key, "1")
        val numFiles = 40
        val rows = (0 until numFiles).map { i =>
          val (bytes, cardinality) = newBitmap(i, i + 5L, i + 1000L)
          DeletionVectorData(s"file-$i.parquet", bytes, cardinality)
        }
        val tablePath = new Path(table.getAbsolutePath)
        val hadoopConf = spark.sessionState.newHadoopConf()
        val results = GpuDeletionVectorWriter.createMapper(
          spark, hadoopConf, tablePath, prefixLength = 0)(rows.iterator).toSeq

        assert(results.map(_.filePath) == rows.map(_.filePath), "results lost their order")
        assert(results.forall(_.matchedRowCount == 3))
        assert(dvFiles(table).length == numFiles)

        val dvStore = DeletionVectorStore.createInstance(hadoopConf)
        results.zipWithIndex.foreach { case (result, i) =>
          val loaded = StoredBitmap.create(result.deletionVector, tablePath).load(dvStore)
          assert(loaded.cardinality == 3)
          Seq(i.toLong, i + 5L, i + 1000L).foreach(v => assert(loaded.contains(v)))
        }
      }
    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(table)
    }
  }

  test("the DV mapper writes concurrently into random prefix directories") {
    val table = Files.createTempDirectory("gpu-dv-writer-prefix").toFile
    try {
      withCpuSparkSession { spark =>
        spark.conf.set(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE.key, "1")
        val numFiles = 40
        val rows = (0 until numFiles).map { i =>
          val (bytes, cardinality) = newBitmap(i, i + 5L)
          DeletionVectorData(s"file-$i.parquet", bytes, cardinality)
        }
        val tablePath = new Path(table.getAbsolutePath)
        val hadoopConf = spark.sessionState.newHadoopConf()
        hadoopConf.set("fs.file.impl", classOf[ThreadRecordingFs].getName)
        hadoopConf.setBoolean("fs.file.impl.disable.cache", true)
        ThreadRecordingFs.threads.clear()
        val results = GpuDeletionVectorWriter.createMapper(
          spark, hadoopConf, tablePath, prefixLength = 2)(rows.iterator).toSeq

        assert(results.map(_.filePath) == rows.map(_.filePath))
        val files = allDvFiles(table)
        assert(files.length == numFiles)
        assert(files.exists(_.getParentFile != table), "no random prefix directory was used")
        // Files of a multi-bin task are written by the writer pool, not by the task thread.
        assert(ThreadRecordingFs.threads.asScala.exists(_.startsWith("gpu-delta-dv-writer")))

        val dvStore = DeletionVectorStore.createInstance(hadoopConf)
        results.zipWithIndex.foreach { case (result, i) =>
          val loaded = StoredBitmap.create(result.deletionVector, tablePath).load(dvStore)
          assert(loaded.cardinality == 2 && loaded.contains(i.toLong) && loaded.contains(i + 5L))
        }
      }
    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(table)
    }
  }

  test("a failure when closing a DV file fails the task") {
    // On an object store close() uploads the file. The failure must not be swallowed, whether the
    // task writes one file or many, or the commit would reference a file that does not exist.
    Seq("1", "100000000").foreach { packingTarget =>
      val table = Files.createTempDirectory("gpu-dv-writer-close").toFile
      try {
        withCpuSparkSession { spark =>
          spark.conf.set(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE.key, packingTarget)
          val hadoopConf = spark.sessionState.newHadoopConf()
          hadoopConf.set("fs.file.impl", classOf[FailOnCloseFs].getName)
          hadoopConf.setBoolean("fs.file.impl.disable.cache", true)
          val rows = (0 until 5).map { i =>
            val (bytes, cardinality) = newBitmap(i)
            DeletionVectorData(s"file-$i.parquet", bytes, cardinality)
          }
          val ex = intercept[IOException] {
            GpuDeletionVectorWriter.createMapper(spark, hadoopConf,
              new Path(table.getAbsolutePath), prefixLength = 0)(rows.iterator).toSeq
          }
          assert(ex.getMessage == "upload failed")
        }
      } finally {
        org.apache.commons.io.FileUtils.deleteQuietly(table)
      }
    }
  }

  test("a limit of one writes every file on the calling thread, like Delta") {
    val table = Files.createTempDirectory("gpu-dv-writer-serial").toFile
    try {
      withCpuSparkSession { spark =>
        spark.conf.set(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE.key, "1")
        spark.conf.set(RapidsConf.DELTA_DV_WRITE_MAX_FILES_IN_FLIGHT_PER_TASK.key, "1")
        val hadoopConf = spark.sessionState.newHadoopConf()
        hadoopConf.set("fs.file.impl", classOf[ThreadRecordingFs].getName)
        hadoopConf.setBoolean("fs.file.impl.disable.cache", true)
        ThreadRecordingFs.threads.clear()
        val numFiles = 10
        val rows = (0 until numFiles).map { i =>
          val (bytes, cardinality) = newBitmap(i)
          DeletionVectorData(s"file-$i.parquet", bytes, cardinality)
        }
        val results = GpuDeletionVectorWriter.createMapper(
          spark, hadoopConf, new Path(table.getAbsolutePath), prefixLength = 0)(rows.iterator)
          .toSeq
        assert(results.map(_.filePath) == rows.map(_.filePath))
        assert(allDvFiles(table).length == numFiles)
        assert(ThreadRecordingFs.threads.asScala.toSet == Set(Thread.currentThread().getName))
      }
    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(table)
    }
  }

  test("the writer reports the files it wrote and their write time as metrics") {
    Seq("1", "8").foreach { limit =>
      val table = Files.createTempDirectory("gpu-dv-writer-metrics").toFile
      try {
        withCpuSparkSession { spark =>
          spark.conf.set(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE.key, "1")
          spark.conf.set(RapidsConf.DELTA_DV_WRITE_MAX_FILES_IN_FLIGHT_PER_TASK.key, limit)
          val metrics = GpuDeletionVectorWriter.WriteMetrics.create(spark)
          val numFiles = 6
          val rows = (0 until numFiles).map { i =>
            val (bytes, cardinality) = newBitmap(i)
            DeletionVectorData(s"file-$i.parquet", bytes, cardinality)
          }
          val mapper = GpuDeletionVectorWriter.createMapper(spark,
            spark.sessionState.newHadoopConf(), new Path(table.getAbsolutePath),
            prefixLength = 0, metrics = Some(metrics))
          val results = mapper(rows.iterator)
          // Nothing is reported before the results are consumed.
          assert(metrics.files.value == 0)
          assert(results.toSeq.length == numFiles)
          assert(metrics.files.value == numFiles, s"limit $limit")
          assert(metrics.writeTimeMs.value >= 0)
        }
      } finally {
        org.apache.commons.io.FileUtils.deleteQuietly(table)
      }
    }
  }

  test("a store failure fails the task and leaves no deletion vector files") {
    val dir = Files.createTempDirectory("gpu-dv-writer-fail").toFile
    try {
      val notADirectory = new File(dir, "table")
      assert(notADirectory.createNewFile())
      withCpuSparkSession { spark =>
        spark.conf.set(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE.key, "1")
        val rows = (0 until 5).map { i =>
          val (bytes, cardinality) = newBitmap(i)
          DeletionVectorData(s"file-$i.parquet", bytes, cardinality)
        }
        intercept[Exception] {
          GpuDeletionVectorWriter.createMapper(spark, spark.sessionState.newHadoopConf(),
            new Path(notADirectory.getAbsolutePath), prefixLength = 0)(rows.iterator).toSeq
        }
        assert(allDvFiles(dir).isEmpty)
      }
    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(dir)
    }
  }

  test("a row whose file path was not matched fails the task") {
    // The left outer join that attaches existing DVs yields a null path for an unmatched file.
    // Persisting must fail on it, here on a writer pool thread, not drop the delete.
    val table = Files.createTempDirectory("gpu-dv-writer-null").toFile
    try {
      withCpuSparkSession { spark =>
        spark.conf.set(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE.key, "1")
        val rows = (0 until 5).map { i =>
          val (bytes, cardinality) = newBitmap(i)
          DeletionVectorData(if (i == 3) null else s"file-$i.parquet", bytes, cardinality)
        }
        // Delta's writer asserts that the path is set.
        intercept[AssertionError] {
          GpuDeletionVectorWriter.createMapper(spark, spark.sessionState.newHadoopConf(),
            new Path(table.getAbsolutePath), prefixLength = 0)(rows.iterator).toSeq
        }
      }
    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(table)
    }
  }

  test("the DV mapper packs like Delta's writer and merges existing deletion vectors") {
    val table = Files.createTempDirectory("gpu-dv-writer-merge").toFile
    try {
      withCpuSparkSession { spark =>
        val tablePath = new Path(table.getAbsolutePath)
        val hadoopConf = spark.sessionState.newHadoopConf()
        val dvStore = DeletionVectorStore.createInstance(hadoopConf)

        // Persist a first generation of DVs for 6 files, a few of them empty.
        val first = (0 until 6).map { i =>
          val (bytes, cardinality) =
            if (i % 3 == 2) newBitmap() else newBitmap(i.toLong)
          DeletionVectorData(s"file-$i.parquet", bytes, cardinality)
        }
        val firstResults = GpuDeletionVectorWriter.createMapper(
          spark, hadoopConf, tablePath, prefixLength = 0)(first.iterator).toSeq

        // Update every file, merging with the existing DV descriptors.
        val second = firstResults.zipWithIndex.map { case (result, i) =>
          val (bytes, cardinality) = newBitmap(100L + i)
          DeletionVectorData(
            result.filePath,
            if (result.deletionVector.cardinality > 0) {
              Some(result.deletionVector.serializeToBase64())
            } else {
              None
            },
            bytes,
            cardinality)
        }
        // A target that packs roughly two bitmaps per file.
        spark.conf.set(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE.key,
          (second.head.deletedRowIndexSet.length * 3).toString)
        def run(mapper: Iterator[DeletionVectorData] => Iterator[DeletionVectorResult]) = {
          val before = dvFiles(table).length
          val res = mapper(second.iterator).toSeq
          (res, dvFiles(table).length - before)
        }
        val (gpuResults, gpuNewFiles) = run(GpuDeletionVectorWriter.createMapper(
          spark, hadoopConf, tablePath, prefixLength = 0))
        val (cpuResults, cpuNewFiles) = run(DeletionVectorWriter.createMapperToStoreDeletionVectors(
          spark, hadoopConf, tablePath, prefixLength = 0))

        assert(gpuNewFiles == cpuNewFiles)
        assert(gpuNewFiles > 1)
        assert(gpuResults.map(_.filePath) == cpuResults.map(_.filePath))
        gpuResults.zip(cpuResults).zipWithIndex.foreach { case ((gpu, cpu), i) =>
          assert(gpu.matchedRowCount == cpu.matchedRowCount)
          assert(gpu.deletionVector.cardinality == cpu.deletionVector.cardinality)
          val gpuBitmap = StoredBitmap.create(gpu.deletionVector, tablePath).load(dvStore)
          val cpuBitmap = StoredBitmap.create(cpu.deletionVector, tablePath).load(dvStore)
          assert(gpuBitmap.toArray.toSeq == cpuBitmap.toArray.toSeq)
          assert(gpuBitmap.contains(100L + i))
        }
      }
    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(table)
    }
  }
}
