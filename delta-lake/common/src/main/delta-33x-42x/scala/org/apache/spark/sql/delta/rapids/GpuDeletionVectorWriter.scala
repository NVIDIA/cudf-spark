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

package org.apache.spark.sql.delta.rapids

import java.security.PrivilegedExceptionAction
import java.util.concurrent.{Callable, ExecutionException, ExecutorService, Future, Semaphore}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.RapidsConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.commands.{DeletionVectorData, DeletionVectorResult,
  DeletionVectorWriter}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.util.{BinPackingIterator, Utils => DeltaUtils}
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils, Utils => SparkUtils}

/**
 * Persists deletion vectors like Delta's `DeletionVectorWriter.createMapperToStoreDeletionVectors`,
 * but overlaps the packed DV files of a task. Delta opens, writes, and closes the files one at a
 * time, so a task with many files is bound by the open/close latency of the storage, not by the
 * work that produced the bitmaps. Output files, packing, and results are the same as Delta's.
 */
private[rapids] object GpuDeletionVectorWriter extends Logging {
  /**
   * Upper bound on writer threads shared by all concurrently running tasks of an executor, however
   * high `maxFilesInFlightPerTask` is set. Bins
   * beyond it queue for a thread, which is safe, as writers never wait for each other: with many
   * DV-writing tasks, each task's files just take longer. Queued bins still hold their data, so
   * the limit bounds threads, not buffered bins. It was not tuned for that case.
   */
  private val MaxWriterThreads = 64

  private lazy val writerPool: ExecutorService =
    ThreadUtils.newDaemonCachedThreadPool("gpu-delta-dv-writer", MaxWriterThreads)

  def createMapper(
      spark: SparkSession,
      hadoopConf: Configuration,
      table: Path,
      prefixLength: Int): Iterator[DeletionVectorData] => Iterator[DeletionVectorResult] = {
    val broadcastHadoopConf =
      spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    // hadoop.fs.Path is not Serializable, so close over its string representation.
    val tablePathString = DeletionVectorStore.pathToEscapedString(table)
    val packingTargetSize = spark.conf.get(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE)
    // Packed files a task writes at once. This is a count, not a size: each file is buffered up to
    // the packing target, so a task buffers up to this many times that, and an executor up to that
    // times its concurrently running tasks. The default comes from S3 measurements, where more
    // files in flight kept helping but cost memory in proportion.
    val maxFilesInFlight =
      new RapidsConf(spark.sessionState.conf).deltaDvWriteMaxFilesInFlightPerTask

    rows => {
      val dvStore = DeletionVectorStore.createInstance(broadcastHadoopConf.value.value)
      val tablePath = DeletionVectorStore.escapedStringToPath(tablePathString)
      val tablePathWithFS = dvStore.pathWithFileSystem(tablePath)
      // Writer threads must write as the user of the task, not as the thread that made the pool.
      val ugi = UserGroupInformation.getCurrentUser

      def writeBin(bin: Seq[DeletionVectorData]): Seq[DeletionVectorResult] = {
        val prefix = DeltaUtils.getRandomPrefix(prefixLength)
        ugi.doAs(new PrivilegedExceptionAction[Seq[DeletionVectorResult]] {
          override def run(): Seq[DeletionVectorResult] = {
            val (writer, fileId) =
              DeletionVectorWriter.createWriter(dvStore, tablePathWithFS, prefix)
            val ctx = DeletionVectorWriter.DeletionVectorMapperContext(
              dvStore, writer, tablePath, fileId, prefix)
            // A failing close must fail the task: on object stores close() uploads the file, and
            // Spark 4.1's tryWithResource would swallow its error (SPARK-53136), committing a
            // reference to a file that does not exist. Same as Delta 4.4.
            SparkUtils.tryWithSafeFinally {
              bin.map(DeletionVectorWriter.storeBitmapAndGenerateResult(ctx, _))
            } {
              writer.close()
            }
          }
        })
      }

      writeBins(new BinPackingIterator(rows, packingTargetSize), writeBin,
        maxInFlight = maxFilesInFlight)
    }
  }

  /**
   * Applies `writeBin` to every bin with up to `maxInFlight` bins in flight. Results keep the bin
   * order. A single bin, the common case, is written on the task thread, and so is every bin when
   * `maxInFlight` is 1, as in Delta's serial writer.
   */
  private[rapids] def writeBins[I, O](
      bins: Iterator[Seq[I]],
      writeBin: Seq[I] => Seq[O],
      maxInFlight: Int,
      pool: => ExecutorService = writerPool): Iterator[O] = {
    if (maxInFlight == 1) {
      bins.flatMap(writeBin)
    } else if (!bins.hasNext) {
      Iterator.empty
    } else {
      val first = bins.next()
      if (!bins.hasNext) {
        writeBin(first).iterator
      } else {
        writeBinsConcurrently(Iterator.single(first) ++ bins, writeBin, pool, maxInFlight)
      }
    }
  }

  private def writeBinsConcurrently[I, O](
      bins: Iterator[Seq[I]],
      writeBin: Seq[I] => Seq[O],
      executor: ExecutorService,
      maxInFlight: Int): Iterator[O] = {
    val slots = new Semaphore(maxInFlight)
    val aborted = new AtomicBoolean(false)
    val futures = ArrayBuffer.empty[Future[Seq[O]]]
    val inFlight = new AtomicInteger(0)
    val peakInFlight = new AtomicInteger(0)
    val startNs = System.nanoTime()
    try {
      bins.takeWhile(_ => !aborted.get).foreach { bin =>
        slots.acquire()
        try {
          futures += executor.submit(new Callable[Seq[O]] {
            override def call(): Seq[O] = try {
              // After a failure the stage fails anyway, so do not start more files.
              if (aborted.get) {
                Seq.empty
              } else {
                peakInFlight.accumulateAndGet(inFlight.incrementAndGet(), math.max(_, _))
                try writeBin(bin) finally inFlight.decrementAndGet()
              }
            } catch {
              case t: Throwable =>
                aborted.set(true)
                throw t
            } finally {
              slots.release()
            }
          })
        } catch {
          case t: Throwable =>
            slots.release()
            throw t
        }
      }
      // Collecting in submission order keeps the results in Delta's order.
      val results = futures.map(_.get())
      // Without this, a saturated writer pool is invisible: its threads are not task threads.
      logInfo(s"Wrote ${futures.size} deletion vector files, at most ${peakInFlight.get} " +
        s"at a time, in ${(System.nanoTime() - startNs) / 1000000} ms")
      results.iterator.flatten
    } catch {
      case t: Throwable =>
        aborted.set(true)
        // Wait for running writers so none still writes after the task has failed or was killed.
        // A killed task therefore exits only after its in-flight files are done. The kill does not
        // interrupt the writer threads, which a serial write on the task thread would have been.
        // The wait is bounded by the storage client's own timeouts, not by this code.
        val primary = unwrap(t)
        var interrupted = t.isInstanceOf[InterruptedException]
        futures.foreach { f =>
          var done = false
          while (!done) {
            try {
              f.get()
              done = true
            } catch {
              case _: InterruptedException => interrupted = true
              case other: Throwable =>
                // Keep the failures of the other writers, which would otherwise be lost.
                val failure = unwrap(other)
                if (failure ne primary) primary.addSuppressed(failure)
                done = true
            }
          }
        }
        if (interrupted) Thread.currentThread().interrupt()
        throw primary
    }
  }

  private def unwrap(t: Throwable): Throwable = t match {
    case e: ExecutionException if e.getCause != null => e.getCause
    case other => other
  }
}
