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

package com.nvidia.spark.rapids

import java.io.{File, InputStream}
import java.nio.channels.ClosedChannelException
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import _root_.io.netty.channel.FileRegion
import com.nvidia.spark.rapids.spill.SpillablePartialFileHandle
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.{ShuffleBlockBatchId, ShuffleBlockId}

class MultithreadedShuffleBufferCatalogSuite
    extends AnyFunSuite with MockitoSugar with BeforeAndAfterEach {

  test("registered shuffles should be active") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    assertResult(false)(catalog.hasActiveShuffle(123))
    catalog.registerShuffle(123)
    assertResult(true)(catalog.hasActiveShuffle(123))
    catalog.unregisterShuffle(123)
    assertResult(false)(catalog.hasActiveShuffle(123))
  }

  test("addPartition and hasData") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandle()

    catalog.registerShuffle(1)

    val blockId = ShuffleBlockId(1, 0L, 0)
    assertResult(false)(catalog.hasData(blockId))

    catalog.addPartition(1, 0L, 0, handle, 0, 100)
    assertResult(true)(catalog.hasData(blockId))

    catalog.unregisterShuffle(1)
    verify(handle).close()
  }

  test("getMergedBuffer returns correct data") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandleWithData(Array[Byte](1, 2, 3, 4, 5))

    catalog.registerShuffle(1)
    catalog.addPartition(1, 0L, 0, handle, 0, 5)

    val buffer = catalog.getMergedBuffer(ShuffleBlockId(1, 0L, 0))
    assertResult(5)(buffer.size())

    val byteBuffer = buffer.nioByteBuffer()
    assertResult(1)(byteBuffer.get(0))
    assertResult(5)(byteBuffer.get(4))

    catalog.unregisterShuffle(1)
  }

  test("getMergedBatchBuffer returns correct data for multiple partitions") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandleWithData(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    catalog.registerShuffle(1)
    // Add 3 partitions from the same handle
    catalog.addPartition(1, 0L, 0, handle, 0, 3)   // bytes 0-2
    catalog.addPartition(1, 0L, 1, handle, 3, 3)   // bytes 3-5
    catalog.addPartition(1, 0L, 2, handle, 6, 4)   // bytes 6-9

    // Request batch containing partitions 0, 1, 2
    val batchId = ShuffleBlockBatchId(1, 0L, 0, 3)
    val buffer = catalog.getMergedBatchBuffer(batchId)
    assertResult(10)(buffer.size())

    catalog.unregisterShuffle(1)
  }

  test("unregisterShuffle closes all handles") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle1 = createMockHandle()
    val handle2 = createMockHandle()

    catalog.registerShuffle(1)
    catalog.addPartition(1, 0L, 0, handle1, 0, 100)
    catalog.addPartition(1, 1L, 0, handle2, 0, 100)

    catalog.unregisterShuffle(1)

    verify(handle1).close()
    verify(handle2).close()
  }

  test("unregisterShuffle closes each handle only once") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandle()

    catalog.registerShuffle(1)
    // Same handle used for multiple partitions
    catalog.addPartition(1, 0L, 0, handle, 0, 100)
    catalog.addPartition(1, 0L, 1, handle, 100, 100)
    catalog.addPartition(1, 0L, 2, handle, 200, 100)

    catalog.unregisterShuffle(1)

    // Should only be closed once
    verify(handle, times(1)).close()
  }

  test("unregisterShuffle handles close exception gracefully") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandle()
    // Use RuntimeException since close() doesn't declare checked exceptions
    doThrow(new RuntimeException("Test exception")).when(handle).close()

    catalog.registerShuffle(1)
    catalog.addPartition(1, 0L, 0, handle, 0, 100)

    // Should not throw
    catalog.unregisterShuffle(1)

    verify(handle).close()
  }

  test("getMergedBuffer throws for non-existent block") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    catalog.registerShuffle(1)

    assertThrows[IllegalArgumentException] {
      catalog.getMergedBuffer(ShuffleBlockId(1, 0L, 0))
    }

    catalog.unregisterShuffle(1)
  }

  test("getMergedBatchBuffer throws for non-existent blocks") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    catalog.registerShuffle(1)

    assertThrows[IllegalArgumentException] {
      catalog.getMergedBatchBuffer(ShuffleBlockBatchId(1, 0L, 0, 3))
    }

    catalog.unregisterShuffle(1)
  }

  test("empty partitions are skipped") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandle()

    catalog.registerShuffle(1)

    // Adding partition with length 0 should be skipped
    catalog.addPartition(1, 0L, 0, handle, 0, 0)
    assertResult(false)(catalog.hasData(ShuffleBlockId(1, 0L, 0)))

    // Adding partition with length > 0 should work
    catalog.addPartition(1, 0L, 1, handle, 0, 100)
    assertResult(true)(catalog.hasData(ShuffleBlockId(1, 0L, 1)))

    catalog.unregisterShuffle(1)
  }

  test("multiple batches for same partition are accumulated") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle1 = createMockHandleWithData(Array[Byte](1, 2, 3))
    val handle2 = createMockHandleWithData(Array[Byte](4, 5, 6))

    catalog.registerShuffle(1)
    catalog.addPartition(1, 0L, 0, handle1, 0, 3)
    catalog.addPartition(1, 0L, 0, handle2, 0, 3)  // Same partition, different batch

    val buffer = catalog.getMergedBuffer(ShuffleBlockId(1, 0L, 0))
    assertResult(6)(buffer.size())  // Should contain data from both batches

    catalog.unregisterShuffle(1)
  }

  // ------------------------------------------------------------------------------------------
  // Regression test: a retained file-backed (FILE_ONLY) skip-merge shuffle buffer must stay
  // readable while shuffle cleanup unregisters the catalog entry. The test retains a buffer,
  // starts concurrent stream readers, unregisters the shuffle while reads are in flight, and
  // verifies that the active readers do not observe a closed backing channel.
  //
  // The race depends on cleanup landing while a reader is using the cached FileChannel, so the
  // test oversubscribes readers and uses small, frequent reads with bounded retries.

  test("retained skip-merge buffer stays readable across concurrent unregisterShuffle") {
    val race = MultithreadedShuffleBufferCatalogSuite.RetainedBufferReadRace
    var bug: Option[Throwable] = None
    var otherError: Option[Throwable] = None
    var sawReadsInFlight = false
    var sawReadsAfterUnregister = false
    var leakedReaders = 0
    var iteration = 0
    while (bug.isEmpty && iteration < race.MaxIterations) {
      iteration += 1
      val result = race.attempt(iteration)
      bug = result.target
      if (otherError.isEmpty) otherError = result.otherError
      sawReadsInFlight ||= result.startedOk && result.readsBeforeUnregister > 0
      sawReadsAfterUnregister ||= result.readsAfterUnregister > 0
      leakedReaders += result.leakedReaders
    }

    bug match {
      case Some(closed) =>
        // BUG: an active read was closed underneath the reader. Surface the real exception so the
        // failure is the ClosedChannelException from the read path, not a synthetic assertion.
        // Expected to FAIL on unmodified `main`.
        throw closed
      case None =>
        // Post-fix expectation: confirm the scenario actually ran and that reads SURVIVED
        // unregisterShuffle, so a future change cannot make this test pass for the wrong reason.
        assert(sawReadsInFlight,
          "reproducer never observed reads before unregisterShuffle; scenario did not run")
        otherError.foreach(e => throw e)
        assert(sawReadsAfterUnregister,
          "no reads completed after unregisterShuffle; the retained buffer was not readable")
        assert(leakedReaders == 0, s"$leakedReaders reader thread(s) did not stop after the test")
        info(s"retained reads survived unregisterShuffle across $iteration iterations")
    }
  }

  test("convertToNetty release closes retained handle exactly once") {
    // The handle owns the close deferral, so a mock can't reproduce it: use a real FILE_ONLY
    // handle and observe the physical close via `isPhysicallyClosed`.
    val catalog = new MultithreadedShuffleBufferCatalog()
    val backingFile = File.createTempFile("skipmerge-region-", ".data")
    val handle = SpillablePartialFileHandle.createFileOnly(backingFile)
    handle.write(Array.fill[Byte](100)(7.toByte), 0, 100)
    handle.finishWrite()
    try {
      catalog.registerShuffle(1)
      catalog.addPartition(1, 0L, 0, handle, 0, 100)

      val buffer = catalog.getMergedBuffer(ShuffleBlockId(1, 0L, 0))
      val region = buffer.convertToNetty().asInstanceOf[FileRegion]

      // Cleanup requests close, but the file region still holds a read lease: close is deferred.
      catalog.unregisterShuffle(1)
      assert(!handle.isPhysicallyClosed,
        "handle must stay open while the file region holds a lease")

      // Releasing the region drops the last lease and runs the deferred physical close.
      assert(region.release())
      assert(handle.isPhysicallyClosed, "handle must close once the file region releases its lease")

      // No retained buffer lease here, so releasing the buffer is a no-op and must not re-close.
      buffer.release()
      assert(handle.isPhysicallyClosed)
    } finally {
      if (backingFile.exists()) {
        backingFile.delete()
      }
    }
  }

  private def createMockHandle(): SpillablePartialFileHandle = {
    val handle = mock[SpillablePartialFileHandle]
    handle
  }

  private def createMockHandleWithData(data: Array[Byte]): SpillablePartialFileHandle = {
    val handle = mock[SpillablePartialFileHandle]
    when(handle.readAt(anyLong(), any[Array[Byte]](), anyInt(), anyInt()))
      .thenAnswer(invocation => {
        val position = invocation.getArgument[Long](0)
        val bytes = invocation.getArgument[Array[Byte]](1)
        val offset = invocation.getArgument[Int](2)
        val length = invocation.getArgument[Int](3)
        val actualLength = math.min(length, (data.length - position).toInt)
        if (actualLength <= 0) {
          -1
        } else {
          System.arraycopy(data, position.toInt, bytes, offset, actualLength)
          actualLength
        }
      })
    handle
  }
}

object MultithreadedShuffleBufferCatalogSuite {
  private object RetainedBufferReadRace {
    // Oversubscribe readers (bounded) so the close reliably lands in a reader's channel-read path.
    private val ReaderThreads: Int =
      math.min(128, math.max(64, Runtime.getRuntime.availableProcessors() * 4))
    private val ReadBufferBytes: Int = 4 * 1024 // small reads => very frequent readAt calls
    private val BackingFileBytes: Int = 2 * 1024 * 1024 // 2 MB single-segment backing file
    private val AwaitSeconds: Long = 30L
    private val PostUnregisterMillis: Long = 200L

    val MaxIterations: Int = 20

    case class Result(
        target: Option[Throwable],
        otherError: Option[Throwable],
        readsBeforeUnregister: Long,
        readsAfterUnregister: Long,
        startedOk: Boolean,
        leakedReaders: Int)

    /** Runs one race iteration and reports what the readers saw. */
    def attempt(iteration: Int): Result = {
      val shuffleId = iteration
      val mapId = 0L
      val reduceId = 0
      val backingFile = File.createTempFile("skipmerge-catalog-repro-", ".data")

      val catalog = new MultithreadedShuffleBufferCatalog()
      val handle = SpillablePartialFileHandle.createFileOnly(backingFile)
      val stop = new AtomicBoolean(false)
      val afterUnregister = new AtomicBoolean(false)
      val started = new CountDownLatch(ReaderThreads)
      val readsBefore = new AtomicLong(0L)
      val readsAfter = new AtomicLong(0L)
      val readerErrors = new ConcurrentLinkedQueue[Throwable]()
      val readers = new ArrayBuffer[Thread](ReaderThreads)
      var buffer: ManagedBuffer = null
      var startedOk = false
      var leakedReaders = 0
      var targetBeforeCleanup: Option[Throwable] = None
      var otherErrorBeforeCleanup: Option[Throwable] = None

      try {
        // Publish a multi-MB file-only handle as a single whole-file segment, like the writer does.
        writeBackingData(handle, BackingFileBytes)
        handle.finishWrite()
        catalog.registerShuffle(shuffleId)
        catalog.addPartition(shuffleId, mapId, reduceId, handle, 0L, BackingFileBytes.toLong)

        // A reducer retains the buffer before handing it to readers; this retained lease is the
        // lifecycle guarantee the regression test protects.
        buffer = catalog.getMergedBuffer(ShuffleBlockId(shuffleId, mapId, reduceId))
        buffer.retain()
        val readBuffer = buffer

        (0 until ReaderThreads).foreach { idx =>
          // A Runnable (not a Thread subclass) so the local `stop` flag is not shadowed by the
          // inherited Thread.stop() member.
          val body = new Runnable {
            override def run(): Unit = {
              var in: InputStream = readBuffer.createInputStream()
              val buf = new Array[Byte](ReadBufferBytes)
              started.countDown()
              try {
                while (!stop.get()) {
                  val n = in.read(buf, 0, buf.length)
                  if (n < 0) {
                    in.close()
                    in = readBuffer.createInputStream()
                  } else if (afterUnregister.get()) {
                    readsAfter.incrementAndGet()
                  } else {
                    readsBefore.incrementAndGet()
                  }
                }
              } catch {
                case NonFatal(t) => readerErrors.add(t)
              } finally {
                try { in.close() } catch { case NonFatal(_) => () }
              }
            }
          }
          val reader = new Thread(body, s"skipmerge-catalog-reader-$iteration-$idx")
          reader.setDaemon(true)
          readers += reader
          reader.start()
        }

        // Make sure reads are flowing before the close, so it lands in a reader's read path.
        startedOk = started.await(AwaitSeconds, TimeUnit.SECONDS) &&
          awaitReadsInFlight(readsBefore, ReaderThreads.toLong)

        // Cleanup thread closes the handle underneath the active readers; then give readers a
        // short window to read the retained buffer post-unregister (on `main` they hit the
        // closed channel; once fixed they keep reading, counted in readsAfter).
        catalog.unregisterShuffle(shuffleId)
        afterUnregister.set(true)
        awaitReadsAfterUnregister(readsAfter, readers, ReaderThreads.toLong)
      } finally {
        stop.set(true)
        leakedReaders = joinAll(readers)
        targetBeforeCleanup = firstMatching(readerErrors, isClosedChannelFromReadPath)
        otherErrorBeforeCleanup =
          firstMatching(readerErrors, t => !isClosedChannelFromReadPath(t))
        // Release the retained buffer before the last-resort handle.close() guard, giving the
        // catalog's deferred close path the first chance to close the handle.
        if (buffer != null) {
          try { buffer.release() } catch { case NonFatal(_) => () }
        }
        try { handle.close() } catch { case NonFatal(_) => () }
        if (backingFile.exists()) {
          backingFile.delete()
        }
      }

      Result(
        target = targetBeforeCleanup,
        otherError = otherErrorBeforeCleanup,
        readsBeforeUnregister = readsBefore.get(),
        readsAfterUnregister = readsAfter.get(),
        startedOk = startedOk,
        leakedReaders = leakedReaders)
    }

    private def writeBackingData(handle: SpillablePartialFileHandle, totalBytes: Int): Unit = {
      val chunk = new Array[Byte](1024 * 1024)
      var i = 0
      while (i < chunk.length) {
        chunk(i) = (i & 0xFF).toByte
        i += 1
      }
      var written = 0
      while (written < totalBytes) {
        val toWrite = math.min(chunk.length, totalBytes - written)
        handle.write(chunk, 0, toWrite)
        written += toWrite
      }
    }

    /** Waits until reads are flowing; returns true if the target read count was reached in time. */
    private def awaitReadsInFlight(reads: AtomicLong, target: Long): Boolean = {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AwaitSeconds)
      while (reads.get() < target && System.nanoTime() < deadline) {
        Thread.sleep(1L)
      }
      reads.get() >= target
    }

    /** Lets readers attempt reads after unregister; stops early once enough succeed or all exit. */
    private def awaitReadsAfterUnregister(
        readsAfter: AtomicLong, readers: ArrayBuffer[Thread], target: Long): Unit = {
      val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(PostUnregisterMillis)
      while (readsAfter.get() < target &&
          System.nanoTime() < deadline && readers.exists(_.isAlive)) {
        Thread.sleep(1L)
      }
    }

    /** Stops/joins all readers under one shared deadline; returns the count still alive. */
    private def joinAll(readers: ArrayBuffer[Thread]): Int = {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AwaitSeconds)
      readers.foreach { reader =>
        val remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())
        if (remainingMs > 0) {
          try {
            reader.join(remainingMs)
          } catch {
            case _: InterruptedException => Thread.currentThread().interrupt()
          }
        }
      }
      readers.count(_.isAlive)
    }

    private def firstMatching(
        errors: ConcurrentLinkedQueue[Throwable], p: Throwable => Boolean): Option[Throwable] = {
      var found: Option[Throwable] = None
      val it = errors.iterator()
      while (found.isEmpty && it.hasNext) {
        val t = it.next()
        if (p(t)) {
          found = Some(t)
        }
      }
      found
    }

    private def isClosedChannelFromReadPath(t: Throwable): Boolean = {
      // AsynchronousCloseException (channel closed while a read is in flight) is a subclass of
      // ClosedChannelException, so this covers both.
      t.isInstanceOf[ClosedChannelException] && t.getStackTrace.exists { frame =>
        frame.getClassName.endsWith("SpillablePartialFileHandle") &&
          (frame.getMethodName.contains("readFromFileChannel") ||
            frame.getMethodName.contains("readAt"))
      }
    }
  }
}
