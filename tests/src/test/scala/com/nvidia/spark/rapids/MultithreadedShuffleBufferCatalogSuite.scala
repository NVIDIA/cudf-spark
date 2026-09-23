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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

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
  import MultithreadedShuffleBufferCatalogSuite.publish

  private case class FileOnlyPartitionFixture(
      catalog: MultithreadedShuffleBufferCatalog,
      handle: SpillablePartialFileHandle,
      backingFile: File,
      blockId: ShuffleBlockId,
      batchId: ShuffleBlockBatchId)

  private case class ConsumerAction(name: String, run: () => (() => Unit))

  private case class PausedCleanup(
      batchId: ShuffleBlockBatchId,
      handles: Seq[SpillablePartialFileHandle],
      pause: () => Unit,
      finish: () => Unit)

  test("registered shuffles should be active") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    assertResult(false)(catalog.hasActiveShuffle(123))
    catalog.registerShuffle(123)
    assertResult(true)(catalog.hasActiveShuffle(123))
    catalog.unregisterShuffle(123)
    assertResult(false)(catalog.hasActiveShuffle(123))
  }

  test("publishMapOutput and hasData") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandle()

    catalog.registerShuffle(1)

    val blockId = ShuffleBlockId(1, 0L, 0)
    assertResult(false)(catalog.hasData(blockId))

    assert(publish(catalog, 1, 0L, (0, handle, 0L, 100L)).isDefined)
    assertResult(true)(catalog.hasData(blockId))

    catalog.unregisterShuffle(1)
    verify(handle).close()
    assertResult(false)(catalog.hasData(blockId))
  }

  test("getMergedBuffer returns correct data") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandleWithData(Array[Byte](1, 2, 3, 4, 5))

    catalog.registerShuffle(1)
    publish(catalog, 1, 0L, (0, handle, 0L, 5L))

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
    // Publish 3 partitions from the same handle
    publish(catalog, 1, 0L,
      (0, handle, 0L, 3L),   // bytes 0-2
      (1, handle, 3L, 3L),   // bytes 3-5
      (2, handle, 6L, 4L))   // bytes 6-9

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
    assert(publish(catalog, 1, 0L, (0, handle1, 0L, 100L)).isDefined)
    assert(publish(catalog, 1, 1L, (0, handle2, 0L, 100L)).isDefined)

    catalog.unregisterShuffle(1)

    verify(handle1).close()
    verify(handle2).close()
  }

  test("unregisterShuffle closes each handle only once") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandle()

    catalog.registerShuffle(1)
    // Same handle used for multiple partitions
    assert(publish(catalog, 1, 0L,
      (0, handle, 0L, 100L),
      (1, handle, 100L, 100L),
      (2, handle, 200L, 100L)).isDefined)

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
    assert(publish(catalog, 1, 0L, (0, handle, 0L, 100L)).isDefined)

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

    // A partition with length 0 is not stored; one with data is
    publish(catalog, 1, 0L, (0, handle, 0L, 0L), (1, handle, 0L, 100L))
    assertResult(false)(catalog.hasData(ShuffleBlockId(1, 0L, 0)))
    assertResult(true)(catalog.hasData(ShuffleBlockId(1, 0L, 1)))

    catalog.unregisterShuffle(1)
  }

  test("getMergedBatchBuffer returns every partition around an empty reduce id") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandleWithData(Array[Byte](1, 1, 1, 1, 2, 2, 2, 2))

    catalog.registerShuffle(1)
    // Reduce 1 is empty, so it is never stored and the batch range spans a gap.
    publish(catalog, 1, 0L, (0, handle, 0L, 4L), (1, handle, 4L, 0L), (2, handle, 4L, 4L))
    assertResult(false)(catalog.hasData(ShuffleBlockId(1, 0L, 1)))

    val buffer = catalog.getMergedBatchBuffer(ShuffleBlockBatchId(1, 0L, 0, 3))
    assertResult(8)(buffer.size())
    val bytes = buffer.nioByteBuffer()
    assertResult(Seq[Byte](1, 1, 1, 1, 2, 2, 2, 2))((0 until 8).map(i => bytes.get(i)))

    catalog.unregisterShuffle(1)
  }

  test("multiple batches for same partition are accumulated") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle1 = createMockHandleWithData(Array[Byte](1, 2, 3))
    val handle2 = createMockHandleWithData(Array[Byte](4, 5, 6))

    catalog.registerShuffle(1)
    publish(catalog, 1, 0L,
      (0, handle1, 0L, 3L),
      (0, handle2, 0L, 3L))  // Same partition, different batch

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
    withFileOnlyPartition("skipmerge-region-") { fixture =>
      val buffer = fixture.catalog.getMergedBuffer(fixture.blockId)
      val region = buffer.convertToNetty().asInstanceOf[FileRegion]
      try {
        // Cleanup requests close, but the file region still holds a read lease: close is deferred.
        fixture.catalog.unregisterShuffle(fixture.blockId.shuffleId)
        assert(!fixture.handle.isPhysicallyClosed,
          "handle must stay open while the file region holds a lease")

        // Releasing the region drops the last lease and runs the deferred physical close.
        assert(region.release())
        assert(fixture.handle.isPhysicallyClosed,
          "handle must close once the file region releases its lease")

        // No retained buffer lease here, so releasing the buffer is a no-op and must not re-close.
        buffer.release()
        assert(fixture.handle.isPhysicallyClosed)
      } finally {
        if (region.refCnt() > 0) {
          region.release()
        }
      }
    }
  }

  test("handed-off single buffer reports missing data when cleanup wins") {
    verifyClosedSingleHandoff()
  }

  test("handed-off batch buffer reports missing data when cleanup wins") {
    verifyClosedBatchHandoff()
  }

  test("batch lookup reports missing data while cleanup is removing the batch") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    withCleanupPaused(catalog) { cleanup =>
      cleanup.pause()
      lookupBatchSize(catalog, cleanup.batchId).foreach { size =>
        fail(s"returned $size of 8 bytes while cleanup was removing the batch")
      }
    }
  }

  test("re-registering during cleanup does not expose part of a map output") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    withCleanupPaused(catalog) { cleanup =>
      val shuffleId = cleanup.batchId.shuffleId
      cleanup.pause()
      // A map task of the same shuffle starting now registers it again, as getWriter does.
      catalog.registerShuffle(shuffleId)
      assert(catalog.hasActiveShuffle(shuffleId))
      lookupBatchSize(catalog, cleanup.batchId).foreach { size =>
        fail(s"returned $size of 8 bytes after the shuffle was re-registered during cleanup")
      }
      val oldBlock = ShuffleBlockId(shuffleId, 0L, 0)
      assert(!catalog.hasData(oldBlock))
      assertThrows[IllegalArgumentException](catalog.getMergedBuffer(oldBlock))

      // A recomputed map output goes into the new registration and is served whole.
      val newHandle = createMockHandleWithData(Array[Byte](9, 9, 9))
      assert(publish(catalog, shuffleId, 1L, (0, newHandle, 0L, 3L)).isDefined)
      cleanup.finish()
      assert(cleanup.handles.forall(_.isPhysicallyClosed))
      verify(newHandle, never()).close()
      val newBatch = ShuffleBlockBatchId(shuffleId, 1L, 0, 3)
      assertResult(3)(catalog.getMergedBatchBuffer(newBatch).size())
      catalog.unregisterShuffle(shuffleId)
    }
  }

  test("a second cleaner of the same shuffle neither waits nor closes anything") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    withCleanupPaused(catalog) { cleanup =>
      cleanup.pause()
      // Executor shutdown can clean a shuffle while the periodic cleanup is still on it.
      val result = new AtomicReference[Option[ShuffleCleanupStats]]()
      val secondCleaner = new Thread(() => {
        result.set(catalog.unregisterShuffle(cleanup.batchId.shuffleId))
      }, "skipmerge-second-cleaner")
      secondCleaner.setDaemon(true)
      secondCleaner.start()
      secondCleaner.join(TimeUnit.SECONDS.toMillis(AwaitSeconds))
      assert(!secondCleaner.isAlive, "the second cleaner waited for the first")
      assert(result.get().isEmpty)
      cleanup.handles.foreach(handle => verify(handle, never()).close())
      cleanup.finish()
      cleanup.handles.foreach(handle => verify(handle, times(1)).close())
    }
  }

  test("a batch buffer retained before cleanup still reads every byte") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    withCleanupPaused(catalog) { cleanup =>
      val buffer = catalog.getMergedBatchBuffer(cleanup.batchId).retain()
      try {
        cleanup.pause()
        cleanup.finish()
        assert(!cleanup.handles.exists(_.isPhysicallyClosed), "the lease must defer the close")
        val bytes = buffer.nioByteBuffer()
        assertResult(Seq[Byte](0, 0, 0, 0, 2, 2, 2, 2))((0 until 8).map(i => bytes.get(i)))
      } finally {
        buffer.release()
      }
      assert(cleanup.handles.forall(_.isPhysicallyClosed))
    }
  }

  test("registering an active shuffle again keeps its map outputs") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandleWithData(Array[Byte](1, 2, 3, 4))
    catalog.registerShuffle(1)
    publish(catalog, 1, 0L, (0, handle, 0L, 4L))
    // Every map task registers the shuffle from getWriter.
    catalog.registerShuffle(1)
    assertResult(4)(catalog.getMergedBatchBuffer(ShuffleBlockBatchId(1, 0L, 0, 1)).size())
    catalog.unregisterShuffle(1)
  }

  test("a map output published after cleanup is refused and closed") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandle()
    val failingHandle = createMockHandle()
    doThrow(new RuntimeException("close failed")).when(failingHandle).close()
    catalog.registerShuffle(1)
    catalog.unregisterShuffle(1)

    // A handle that fails to close must not stop the others or escape from publish.
    assert(publish(catalog, 1, 0L, (0, failingHandle, 0L, 10L), (1, handle, 0L, 10L)).isEmpty)
    verify(failingHandle).close()
    verify(handle).close()
    assert(!catalog.hasData(ShuffleBlockId(1, 0L, 1)))
    assert(!catalog.hasActiveShuffle(1))
  }

  test("a registration closed by cleanup refuses new map outputs") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    catalog.registerShuffle(1)
    // A writer that looked up the registration before cleanup publishes after it.
    val registration = catalog.registration(1)
    catalog.unregisterShuffle(1)
    val output = new MapOutputSegments.Builder().add(0, createMockHandle(), 0L, 10L).build()
    assert(registration.tryPublish(0L, output).isEmpty)
    assert(registration.output(0L) == null)
  }

  test("a map output lands in a registration made after cleanup closed the one it found") {
    val catalog = spy(new MultithreadedShuffleBufferCatalog())
    catalog.registerShuffle(1)
    val lookups = new AtomicInteger(0)
    doAnswer(invocation => {
      val found = invocation.callRealMethod()
      if (lookups.getAndIncrement() == 0) {
        // Cleanup and another map task's registration land between lookup and publish.
        catalog.unregisterShuffle(1)
        catalog.registerShuffle(1)
      }
      found
    }).when(catalog).registration(anyInt())
    val handle = createMockHandleWithData(Array[Byte](1, 2))

    assert(publish(catalog, 1, 0L, (0, handle, 0L, 2L)).isDefined)
    assert(lookups.get() >= 2, "publish never looked the registration up again")
    verify(handle, never()).close()
    assertResult(2)(catalog.getMergedBuffer(ShuffleBlockId(1, 0L, 0)).size())
    catalog.unregisterShuffle(1)
  }

  test("a second output for the same map id is discarded and the first one is kept") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val first = createMockHandleWithData(Array[Byte](1, 1))
    val second = createMockHandleWithData(Array[Byte](2, 2, 2))
    doThrow(new RuntimeException("close failed")).when(second).close()
    catalog.registerShuffle(1)
    val kept = publish(catalog, 1, 0L, (0, first, 0L, 2L))
    // Spark keeps the first committed attempt of a map. A re-run under the old fetch protocol
    // reuses the map id and reports the kept output's lengths.
    assert(publish(catalog, 1, 0L, (0, second, 0L, 3L)) == kept)
    verify(second).close()
    verify(first, never()).close()
    assertResult(2)(catalog.getMergedBuffer(ShuffleBlockId(1, 0L, 0)).size())
    assertResult(Seq(2L, 0L))(kept.get.partitionLengths(2).toSeq)
    catalog.unregisterShuffle(1)
  }

  test("a publish racing cleanup is refused when the close gets the registration first") {
    val state = new ShuffleState()
    val output = new MapOutputSegments.Builder().add(0, createMockHandle(), 0L, 10L).build()
    val result = new AtomicReference[Option[MapOutputSegments]]()
    val publisher =
      new Thread(() => result.set(state.tryPublish(0L, output)), "skipmerge-publisher")
    publisher.setDaemon(true)
    state.synchronized {
      publisher.start()
      // Close while the publish waits for the registration's monitor.
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AwaitSeconds)
      while (publisher.getState != Thread.State.BLOCKED && System.nanoTime() < deadline) {
        Thread.sleep(1)
      }
      assert(publisher.getState == Thread.State.BLOCKED, "the publish did not wait for the close")
      assert(state.close().isEmpty)
    }
    publisher.join(TimeUnit.SECONDS.toMillis(AwaitSeconds))
    assertResult(None)(result.get())
    assert(state.output(0L) == null)
  }

  test("cleanup closes a registration in the same step that detaches it") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    catalog.registerShuffle(1)
    val registration = catalog.registration(1)
    val cleanup = new Thread(() => catalog.unregisterShuffle(1), "skipmerge-detach-cleanup")
    cleanup.setDaemon(true)
    registration.synchronized {
      cleanup.start()
      // Holding the registration's monitor stops cleanup at its close.
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AwaitSeconds)
      while (cleanup.getState != Thread.State.BLOCKED && System.nanoTime() < deadline) {
        Thread.sleep(1)
      }
      assert(cleanup.getState == Thread.State.BLOCKED, "cleanup never reached the close")
      // A writer that finds the registration now publishes into one cleanup will close; it must
      // never find it detached yet open.
      assert(catalog.registration(1) eq registration, "cleanup detached before closing")
    }
    cleanup.join(TimeUnit.SECONDS.toMillis(AwaitSeconds))
    assert(!cleanup.isAlive, "cleanup did not finish")
    assert(catalog.registration(1) == null)
    val output = new MapOutputSegments.Builder().add(0, createMockHandle(), 0L, 10L).build()
    assert(registration.tryPublish(0L, output).isEmpty)
  }

  test("an empty map output takes part in first-wins in both orders") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val empty = new MapOutputSegments.Builder().build()
    catalog.registerShuffle(1)
    // Map 0: the attempt with data comes first, so a later empty attempt reports its lengths.
    val first = new MapOutputSegments.Builder().addPartialFile(createMockHandle(), Array(0L, 6L))
      .build()
    assertResult(Seq(0L, 6L))(catalog.publishMapOutputOrFail(1, 0L, first, 2).toSeq)
    assertResult(Seq(0L, 6L))(catalog.publishMapOutputOrFail(1, 0L, empty, 2).toSeq)
    // Map 1: the empty attempt comes first, so a later attempt with data is discarded.
    val laterHandle = createMockHandle()
    val later = new MapOutputSegments.Builder().addPartialFile(laterHandle, Array(4L, 0L)).build()
    assertResult(Seq(0L, 0L))(catalog.publishMapOutputOrFail(1, 1L, empty, 2).toSeq)
    assertResult(Seq(0L, 0L))(catalog.publishMapOutputOrFail(1, 1L, later, 2).toSeq)
    verify(laterHandle).close()
    assert(!catalog.hasData(ShuffleBlockId(1, 1L, 0)))
    catalog.unregisterShuffle(1)
  }

  test("the writer's publish fails the task after cleanup and closes its output") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandle()
    catalog.registerShuffle(1)
    catalog.unregisterShuffle(1)
    val output = new MapOutputSegments.Builder().addPartialFile(handle, Array(0L, 10L)).build()
    val error = intercept[IllegalStateException] {
      catalog.publishMapOutputOrFail(1, 0L, output, 2)
    }
    assert(error.getMessage.contains("cleaned up"))
    verify(handle).close()
  }

  test("the writer reports the lengths of the map output the catalog keeps") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val first = createMockHandle()
    val second = createMockHandle()
    catalog.registerShuffle(1)
    val firstOutput =
      new MapOutputSegments.Builder().addPartialFile(first, Array(5L, 0L, 7L)).build()
    val secondOutput =
      new MapOutputSegments.Builder().addPartialFile(second, Array(0L, 9L, 0L)).build()
    assertResult(Seq(5L, 0L, 7L))(catalog.publishMapOutputOrFail(1, 0L, firstOutput, 3).toSeq)
    // A re-run of the map id keeps the first output, so its MapStatus must describe that one.
    assertResult(Seq(5L, 0L, 7L))(catalog.publishMapOutputOrFail(1, 0L, secondOutput, 3).toSeq)
    verify(second).close()
    verify(first, never()).close()
    catalog.unregisterShuffle(1)
  }

  test("batch lookup returns exactly the requested reduce ids") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    // Reduces 1, 3, 4 and 7 hold data, first seen out of order across two partial files: file 0
    // holds 3 and 7, file 1 holds 1, 3 and 4. Each byte value is the reduce id then the file.
    val file0 = createMockHandleWithData(Array[Byte](30, 70))
    val file1 = createMockHandleWithData(Array[Byte](11, 31, 41))
    catalog.registerShuffle(1)
    catalog.publishMapOutput(1, 0L, new MapOutputSegments.Builder()
      .addPartialFile(file0, Array(0L, 0L, 0L, 1L, 0L, 0L, 0L, 1L))
      .addPartialFile(file1, Array(0L, 1L, 0L, 1L, 1L, 0L, 0L, 0L))
      .build())

    def read(start: Int, end: Int): Option[Seq[Byte]] = {
      try {
        val bytes = catalog.getMergedBatchBuffer(ShuffleBlockBatchId(1, 0L, start, end))
          .nioByteBuffer()
        Some((0 until bytes.remaining()).map(i => bytes.get(i)))
      } catch {
        case _: IllegalArgumentException => None
      }
    }
    Seq(
      (0, 2) -> Some(Seq[Byte](11)),
      (2, 4) -> Some(Seq[Byte](30, 31)),
      (2, 3) -> None,
      (4, 7) -> Some(Seq[Byte](41)),
      (5, 7) -> None,
      (8, 10) -> None,
      (0, 1) -> None,
      (0, 10) -> Some(Seq[Byte](11, 30, 31, 41, 70))
    ).foreach { case ((start, end), expected) =>
      assertResult(expected, s"batch [$start, $end)")(read(start, end))
    }
    (0 until 10).foreach { reduceId =>
      val blockId = ShuffleBlockId(1, 0L, reduceId)
      assertResult(Set(1, 3, 4, 7).contains(reduceId), blockId)(catalog.hasData(blockId))
    }
    assertResult(Seq(0L, 1L, 0L, 2L, 1L, 0L, 0L, 1L))(
      catalog.registration(1).output(0L).partitionLengths(8).toSeq)
    catalog.unregisterShuffle(1)
  }

  test("unregisterShuffle closes a partial file handle that holds no data") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val withData = createMockHandle()
    val allEmpty = createMockHandle()
    catalog.registerShuffle(1)
    assert(catalog.publishMapOutput(1, 0L, new MapOutputSegments.Builder()
      .addPartialFile(withData, Array(0L, 10L))
      .addPartialFile(allEmpty, Array(0L, 0L))
      .build()).isDefined)
    catalog.unregisterShuffle(1)
    verify(withData).close()
    verify(allEmpty).close()
  }

  test("unrelated lease acquisition failures propagate unchanged") {
    val catalog = new MultithreadedShuffleBufferCatalog()
    val handle = createMockHandle()
    val blockId = ShuffleBlockId(1, 0L, 0)
    val failure = new IllegalStateException("unrelated lease failure")
    doThrow(failure).when(handle).acquireRead()
    try {
      catalog.registerShuffle(1)
      publish(catalog, 1, 0L, (0, handle, 0L, 100L))

      // Only acquire-after-close means missing data. Other failures can expose broken invariants.
      val buffer = catalog.getMergedBuffer(blockId)
      val error = intercept[IllegalStateException](buffer.retain())
      assert(error eq failure)
    } finally {
      catalog.unregisterShuffle(1)
    }
  }

  test("lease acquisition rolls back an earlier handle when a later handle fails") {
    val first = createMockHandle()
    val second = createMockHandle()
    val failure = new IllegalStateException("later acquire failed")
    doThrow(failure).when(second).acquireRead()

    val error = intercept[IllegalStateException] {
      ShuffleHandleLease.acquire(Seq(first, second))
    }

    assert(error eq failure)
    verify(first).acquireRead()
    verify(first, times(1)).releaseRead()
    verify(second).acquireRead()
    verify(second, never()).releaseRead()
  }

  test("unconsumed handed-off buffer does not defer cleanup") {
    withFileOnlyPartition("skipmerge-abandoned-") { fixture =>
      // Creating but not retaining or consuming the buffer must leave cleanup unblocked.
      fixture.catalog.getMergedBuffer(fixture.blockId)

      fixture.catalog.unregisterShuffle(fixture.blockId.shuffleId)
      assert(fixture.handle.isPhysicallyClosed, "an unconsumed buffer pinned the handle")
      assert(!fixture.backingFile.exists(),
        "cleanup did not delete the unconsumed buffer's file")
    }
  }

  private def verifyClosedSingleHandoff(): Unit = {
    verifyClosedHandoff { fixture =>
      val blockId = fixture.blockId
      (fixture.catalog.getMergedBuffer(blockId), s"No data found for block $blockId")
    }
  }

  private def verifyClosedBatchHandoff(): Unit = {
    verifyClosedHandoff { fixture =>
      val batchId = fixture.batchId
      (fixture.catalog.getMergedBatchBuffer(batchId),
        s"No data found for batch block $batchId")
    }
  }

  private def verifyClosedHandoff(
      getBufferAndMessage: FileOnlyPartitionFixture => (ManagedBuffer, String)): Unit = {
    withFileOnlyPartition("skipmerge-handoff-") { fixture =>
      val (buffer, expectedMessage) = getBufferAndMessage(fixture)
      assert(buffer.size() == 100, "buffer was not handed out before cleanup")

      fixture.catalog.unregisterShuffle(fixture.blockId.shuffleId)
      assert(fixture.handle.isPhysicallyClosed, "cleanup did not win the hand-off window")

      val consumerActions = Seq(
        ConsumerAction("retain", () => {
          val retained = buffer.retain()
          () => {
            retained.release()
            ()
          }
        }),
        ConsumerAction("nioByteBuffer", () => {
          buffer.nioByteBuffer()
          () => ()
        }),
        ConsumerAction("createInputStream", () => {
          val input = buffer.createInputStream()
          () => input.close()
        }),
        ConsumerAction("convertToNetty", () => {
          val region = buffer.convertToNetty().asInstanceOf[FileRegion]
          () => {
            region.release()
            ()
          }
        }))

      val attemptedActions = new ArrayBuffer[String](consumerActions.size)
      val firstActionFailure = runAllCapturingFirstFailure(consumerActions.map { action =>
        () => {
          attemptedActions += action.name
          verifyMissingConsumerAction(action, expectedMessage)
        }
      })
      assertResult(consumerActions.map(_.name))(attemptedActions.toSeq)
      firstActionFailure.foreach(throw _)
    }
  }

  private def verifyMissingConsumerAction(
      action: ConsumerAction,
      expectedMessage: String): Unit = withClue(s"${action.name}: ") {
    val result: Either[IllegalArgumentException, () => Unit] =
      try {
        Right(action.run())
      } catch {
        case e: IllegalArgumentException => Left(e)
      }

    result match {
      case Left(error) =>
        assertResult(expectedMessage)(error.getMessage)
        assertResult("com.nvidia.spark.rapids.spill.ClosedPartialFileHandleException")(
          Option(error.getCause).map(_.getClass.getName).orNull)
      case Right(cleanup) =>
        cleanup()
        fail(s"${action.name} did not report the missing block")
    }
  }

  private def runAllCapturingFirstFailure(actions: Seq[() => Unit]): Option[Throwable] = {
    var firstFailure: Option[Throwable] = None
    actions.foreach { action =>
      try {
        action()
      } catch {
        case NonFatal(t) if firstFailure.isEmpty => firstFailure = Some(t)
        case NonFatal(t) => firstFailure.foreach(_.addSuppressed(t))
      }
    }
    firstFailure
  }

  private def withFileOnlyPartition[T](
      filePrefix: String)(body: FileOnlyPartitionFixture => T): T = {
    val backingFile = File.createTempFile(filePrefix, ".data")
    try {
      val shuffleId = 1
      val catalog = new MultithreadedShuffleBufferCatalog()
      val handle = SpillablePartialFileHandle.createFileOnly(backingFile)
      try {
        handle.write(Array.fill[Byte](100)(7.toByte), 0, 100)
        handle.finishWrite()

        val blockId = ShuffleBlockId(shuffleId, 0L, 0)
        val batchId = ShuffleBlockBatchId(shuffleId, 0L, 0, 1)
        catalog.registerShuffle(shuffleId)
        publish(catalog, shuffleId, 0L, (0, handle, 0L, 100L))

        body(FileOnlyPartitionFixture(catalog, handle, backingFile, blockId, batchId))
      } finally {
        try {
          if (catalog.hasActiveShuffle(shuffleId)) {
            catalog.unregisterShuffle(shuffleId)
          }
        } finally {
          handle.close()
        }
      }
    } finally {
      if (backingFile.exists()) {
        backingFile.delete()
      }
    }
  }

  private val AwaitSeconds = 30L

  /**
   * Publishes reduces 0 and 2 of the batch [0, 3) on separate FILE_ONLY handles, leaving reduce 1
   * empty, and hands `body` a cleanup of that shuffle that it can start and pause at the first
   * handle-stats read, which comes after the shuffle is detached and before any handle closes.
   */
  private def withCleanupPaused(catalog: MultithreadedShuffleBufferCatalog)(
      body: PausedCleanup => Unit): Unit = {
    val shuffleId = 1
    val paused = new CountDownLatch(1)
    val resume = new CountDownLatch(1)
    val cleanupFailure = new AtomicReference[Throwable]()
    val cleanup = new Thread(() => {
      try {
        catalog.unregisterShuffle(shuffleId)
      } catch {
        case t: Throwable => cleanupFailure.set(t)
      }
    }, "skipmerge-partial-batch-cleanup")
    cleanup.setDaemon(true)
    val files = new ArrayBuffer[File]()
    val handles = new ArrayBuffer[SpillablePartialFileHandle]()

    def pause(): Unit = {
      cleanup.start()
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AwaitSeconds)
      while (cleanup.isAlive && System.nanoTime() < deadline &&
          !paused.await(10, TimeUnit.MILLISECONDS)) {
        // Poll so that a cleanup thread dying before it pauses fails the test at once.
      }
      if (paused.getCount > 0) {
        Option(cleanupFailure.get()).foreach(throw _)
        fail("cleanup never paused at a handle-stats read")
      }
      assert(!catalog.hasActiveShuffle(shuffleId), "cleanup must detach the shuffle first")
      assert(!handles.exists(_.isPhysicallyClosed), "cleanup must pause before closing handles")
    }

    def finish(): Unit = {
      assert(cleanup.getState != Thread.State.NEW, "finish() called before pause()")
      resume.countDown()
      cleanup.join(TimeUnit.SECONDS.toMillis(AwaitSeconds))
      assert(!cleanup.isAlive, "cleanup did not finish")
      Option(cleanupFailure.get()).foreach(throw _)
    }

    try {
      catalog.registerShuffle(shuffleId)
      val output = new MapOutputSegments.Builder()
      Seq(0, 2).foreach { reduceId =>
        val file = File.createTempFile(s"skipmerge-partial-$reduceId-", ".data")
        files += file
        // Separate handles, as with multi-batch writes, where a partial result stays readable.
        val handle = spy(SpillablePartialFileHandle.createFileOnly(file))
        handles += handle
        handle.write(Array.fill[Byte](4)(reduceId.toByte), 0, 4)
        handle.finishWrite()
        output.add(reduceId, handle, 0L, 4L)
      }
      catalog.publishMapOutput(shuffleId, 0L, output.build())
      handles.foreach { handle =>
        doAnswer(_ => {
          if (Thread.currentThread() eq cleanup) {
            paused.countDown()
            assert(resume.await(AwaitSeconds, TimeUnit.SECONDS))
          }
          4L
        }).when(handle).getTotalBytesWritten
      }
      body(PausedCleanup(ShuffleBlockBatchId(shuffleId, 0L, 0, 3), handles.toList,
        () => pause(), () => finish()))
      if (cleanup.getState != Thread.State.NEW) {
        finish()
      }
    } finally {
      resume.countDown()
      cleanup.join(TimeUnit.SECONDS.toMillis(AwaitSeconds))
      handles.foreach(h => try h.close() catch { case NonFatal(_) => () })
      files.foreach(f => if (f.exists()) f.delete())
    }
  }

  /** Returns the batch buffer's size, or None when the lookup reports missing data. */
  private def lookupBatchSize(
      catalog: MultithreadedShuffleBufferCatalog,
      batchId: ShuffleBlockBatchId): Option[Long] = {
    try {
      Some(catalog.getMergedBatchBuffer(batchId).size())
    } catch {
      case e: IllegalArgumentException =>
        assertResult(s"No data found for batch block $batchId")(e.getMessage)
        None
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
  /** Publishes one map output made of (reduceId, handle, offset, length) segments. */
  private def publish(
      catalog: MultithreadedShuffleBufferCatalog,
      shuffleId: Int,
      mapId: Long,
      segments: (Int, SpillablePartialFileHandle, Long, Long)*): Option[MapOutputSegments] = {
    val output = new MapOutputSegments.Builder()
    segments.foreach { case (reduceId, handle, offset, length) =>
      output.add(reduceId, handle, offset, length)
    }
    catalog.publishMapOutput(shuffleId, mapId, output.build())
  }

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
        publish(catalog, shuffleId, mapId, (reduceId, handle, 0L, BackingFileBytes.toLong))

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
