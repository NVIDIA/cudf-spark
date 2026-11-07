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

package org.apache.spark.sql.rapids.execution

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream,
  ObjectOutputStream}
import java.util.concurrent.{Callable, CountDownLatch, Executors, ExecutorService, Future,
  TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import ai.rapids.cudf.{Rmm, RmmAllocationMode, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuDeviceManager, GpuSemaphore, NoopMetric,
  PrioritySemaphore, SpillableColumnarBatch, SpillableColumnarBatchFromBufferImpl, TestUtils}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.spill.{SpillFramework, SpillUnitTestBase}
import org.apache.commons.lang3.SerializationUtils
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Spark's `MemoryStore` serializes an evicted broadcast block while it holds the memory manager
 * lock, which GPU tasks may also need. Writing a materialized GPU broadcast must therefore not
 * wait for the GPU semaphore (https://github.com/NVIDIA/cudf-spark/issues/16460).
 */
class BroadcastEvictionSuite extends SpillUnitTestBase with Eventually {
  private var rmmWasInitialized = false

  override def beforeEach(): Unit = {
    // Tracks device allocations so tests can check that eviction makes none. Initialized before
    // the spill framework so that RMM also tracks its bounce buffers.
    if (!Rmm.isInitialized) {
      rmmWasInitialized = true
      Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
    }
    super.beforeEach()
    // The semaphore initializes the GPU for a task from the SparkEnv.
    SparkSession.builder.master("local[1]").appName(getClass.getSimpleName).getOrCreate()
    GpuDeviceManager.setRmmTaskInitEnabled(false)
    GpuSemaphore.shutdown()
    GpuSemaphore.initialize(maxConcurrentGpuTasksLimit = 1)
  }

  override def afterEach(): Unit = {
    try {
      GpuSemaphore.shutdown()
      SparkSession.getActiveSession.foreach(_.stop())
      SparkSession.clearActiveSession()
      super.afterEach()
    } finally {
      if (rmmWasInitialized) {
        rmmWasInitialized = false
        Rmm.shutdown()
      }
    }
  }

  private def buildBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1, 1, 1, 1, 1, 1, 1)
        .column("five", "two", null, null, "one", "one", "one", "one", "one", "one")
        .column(5.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
        .build()) { table =>
      GpuColumnVector.from(table, Array(IntegerType, StringType, DoubleType))
    }
  }

  private def makeBroadcastBatch(
      gpuBatch: ColumnarBatch): SerializeConcatHostBuffersDeserializeBatch = {
    val attrs = GpuColumnVector.extractTypes(gpuBatch).map(t => AttributeReference("", t)())
    val buffer = withResource(new SerializeBatchDeserializeHostBuffer(gpuBatch)) { obj =>
      SerializationUtils.clone(obj)
    }
    GpuBroadcastExchangeExecBase.makeBroadcastBatch(
      Array(buffer), attrs, NoopMetric, NoopMetric, NoopMetric, SQLConf.get)
  }

  private def withBroadcast[T](b: SerializeConcatHostBuffersDeserializeBatch)
      (body: SerializeConcatHostBuffersDeserializeBatch => T): T = {
    try body(b) finally b.closeInternal()
  }

  private def write(broadcast: SerializeConcatHostBuffersDeserializeBatch): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    withResource(new ObjectOutputStream(baos)) { oos =>
      broadcast.doWriteObject(oos)
    }
    baos.toByteArray
  }

  private def read(
      bytes: Array[Byte],
      broadcast: SerializeConcatHostBuffersDeserializeBatch =
        new SerializeConcatHostBuffersDeserializeBatch(null, Seq.empty, 0, 0L))
      : SerializeConcatHostBuffersDeserializeBatch = {
    withResource(new ObjectInputStream(new ByteArrayInputStream(bytes))) { ois =>
      broadcast.doReadObject(ois)
    }
    broadcast
  }

  private def assertSameData(
      expected: ColumnarBatch,
      broadcast: SerializeConcatHostBuffersDeserializeBatch): Unit = {
    withResource(broadcast.batch.getColumnarBatch()) { actual =>
      TestUtils.compareBatches(expected, actual)
    }
  }

  /** Interrupt the tasks still running in `pool` and check that they stop. */
  private def stop(pool: ExecutorService): Unit = {
    pool.shutdownNow()
    assert(pool.awaitTermination(30, TimeUnit.SECONDS), "a task did not stop")
  }

  private def awaitOrFail[T](future: Future[T], failure: String): T = {
    try {
      future.get(30, TimeUnit.SECONDS)
    } catch {
      case _: TimeoutException => fail(failure)
    }
  }

  /** Run `body` while another task holds the only GPU permit. */
  private def whileGpuIsBusy[T](body: => T): T = {
    val holderContext = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
    GpuSemaphore.acquireIfNecessary(holderContext)
    // Completing the holder releases its permit, which also ends a deadlock.
    try body finally holderContext.markTaskComplete()
  }

  /**
   * Run `body` like a `MemoryStore` eviction, on a task thread that holds the memory manager
   * lock, and check that it does not allocate device memory.
   */
  private def evict[T](failure: String)(body: => T): T = {
    val evictorContext = new MockTaskContext(taskAttemptId = 2, partitionId = 1)
    val pool = Executors.newSingleThreadExecutor()
    try {
      val evictor = pool.submit(new Callable[T] {
        override def call(): T = {
          TrampolineUtil.setTaskContext(evictorContext)
          try {
            SparkEnv.get.memoryManager.synchronized {
              val allocated = Rmm.getTotalBytesAllocated
              Rmm.resetScopedMaximumBytesAllocated(allocated)
              val result = body
              assert(Rmm.getScopedMaximumBytesAllocated == allocated,
                "eviction allocated device memory")
              result
            }
          } finally {
            TrampolineUtil.unsetTaskContext()
          }
        }
      })
      awaitOrFail(evictor, failure)
    } finally {
      // Interrupt a stuck eviction so that it releases the memory manager lock
      try stop(pool) finally evictorContext.markTaskComplete()
    }
  }

  private val gpuDeadlock = "deadlock: eviction holds the memory manager lock and waits for the GPU"

  /** Number of spill handles on the device, in host memory, and on disk. */
  private def residency: (Int, Int, Int) = {
    val stores = SpillFramework.stores
    (stores.deviceStore.numHandles, stores.hostStore.numHandles, stores.diskStore.numHandles)
  }

  Seq(("on the device", (1, 0, 0)), ("in host memory", (0, 1, 0)), ("on disk", (0, 0, 1))).foreach {
    case (location, expectedResidency) =>
      test(s"evicting a broadcast whose GPU batch is $location does not wait for the GPU") {
        withResource(buildBatch()) { expected =>
          withBroadcast(makeBroadcastBatch(expected)) { broadcast =>
            val size = broadcast.batch.sizeInBytes
            assert(broadcast.batch.isInstanceOf[SpillableColumnarBatchFromBufferImpl])
            if (expectedResidency._1 == 0) {
              assert(SpillFramework.stores.deviceStore.spill(size) > 0)
            }
            if (expectedResidency._3 == 1) {
              assert(SpillFramework.stores.hostStore.spill(size) > 0)
            }
            assert(residency == expectedResidency)
            // Evict it, then evict the reloaded copy before any task materializes it
            val (bytes, bytesAgain) = whileGpuIsBusy(evict(gpuDeadlock) {
              val bytes = write(broadcast)
              (bytes, withBroadcast(read(bytes))(write))
            })
            assert(residency == expectedResidency)
            withBroadcast(read(bytes))(assertSameData(expected, _))
            withBroadcast(read(bytesAgain))(assertSameData(expected, _))
            assertSameData(expected, broadcast)
            // Evict a copy that a task materialized from the packed bytes
            withBroadcast(read(bytes)) { reloaded =>
              reloaded.batch
              val bytesRebuilt = whileGpuIsBusy(evict(gpuDeadlock)(write(reloaded)))
              withBroadcast(read(bytesRebuilt))(assertSameData(expected, _))
            }
          }
        }
      }
  }

  Seq[(String, () => ColumnarBatch)](
    ("zero-row", () => GpuColumnVector.emptyBatchFromTypes(Array(IntegerType, DoubleType))),
    ("rows-only", () => new ColumnarBatch(Array.empty, 1234))).foreach { case (kind, build) =>
    test(s"evicting a materialized $kind broadcast does not wait for the GPU") {
      withResource(build()) { expected =>
        withBroadcast(makeBroadcastBatch(expected)) { broadcast =>
          broadcast.batch
          val bytes = whileGpuIsBusy(evict(gpuDeadlock)(write(broadcast)))
          withBroadcast(read(bytes))(assertSameData(expected, _))
        }
      }
    }
  }

  /**
   * Wait until `thread` is blocked in the GPU semaphore. A blocked task cannot signal this, so
   * poll its state.
   */
  private def awaitWaitingForGpu(thread: AtomicReference[Thread]): Unit = {
    eventually(timeout(Span(30, Seconds))) {
      val t = thread.get()
      assert(t != null && t.getState == Thread.State.WAITING)
      assert(t.getStackTrace.exists(_.getClassName == classOf[PrioritySemaphore[_]].getName))
    }
  }

  test("evicting a broadcast does not wait for a task copying it to the host") {
    withResource(buildBatch()) { expected =>
      withBroadcast(makeBroadcastBatch(expected)) { broadcast =>
        broadcast.batch
        val readerContext = new MockTaskContext(taskAttemptId = 3, partitionId = 2)
        val pool = Executors.newSingleThreadExecutor()
        try {
          val (reader, bytes) = whileGpuIsBusy {
            val readerThread = new AtomicReference[Thread]()
            // A task waits for the GPU while it copies the broadcast to the host
            val reader = pool.submit(new Callable[ColumnarBatch] {
              override def call(): ColumnarBatch = {
                readerThread.set(Thread.currentThread())
                TrampolineUtil.setTaskContext(readerContext)
                try broadcast.hostBatch finally TrampolineUtil.unsetTaskContext()
              }
            })
            awaitWaitingForGpu(readerThread)
            (reader, evict("deadlock: eviction waits for a task copying the broadcast to the host")(
              write(broadcast)))
          }
          val expectedHost = new ColumnarBatch(
            GpuColumnVector.extractColumns(expected).safeMap(_.copyToHost(): ColumnVector),
            expected.numRows())
          withResource(expectedHost) { _ =>
            withResource(awaitOrFail(reader, "the task did not copy the broadcast")) { actual =>
              TestUtils.compareBatches(expectedHost, actual)
            }
          }
          withBroadcast(read(bytes))(assertSameData(expected, _))
        } finally {
          readerContext.markTaskComplete()
          stop(pool)
        }
      }
    }
  }

  Seq(false, true).foreach { packed =>
    test(s"evicting a broadcast does not wait for a task building its GPU batch, " +
        s"packed=$packed") {
      withResource(buildBatch()) { expected =>
        val bytes = withBroadcast(makeBroadcastBatch(expected)) { b =>
          if (packed) {
            b.batch
          }
          write(b)
        }
        val building = new CountDownLatch(1)
        val mayBuild = new CountDownLatch(1)
        // A task pauses while building the GPU batch, as it would while waiting for device memory
        val broadcast = read(bytes,
          new SerializeConcatHostBuffersDeserializeBatch(null, Seq.empty, 0, 0L) {
            override protected def buildGpuBatch(): SpillableColumnarBatch = {
              building.countDown()
              assert(mayBuild.await(30, TimeUnit.SECONDS))
              super.buildGpuBatch()
            }
          })
        withBroadcast(broadcast) { _ =>
          val pool = Executors.newSingleThreadExecutor()
          try {
            val materializer = pool.submit(new Callable[SpillableColumnarBatch] {
              override def call(): SpillableColumnarBatch = broadcast.batch
            })
            assert(building.await(30, TimeUnit.SECONDS))
            val evicted = evict("deadlock: eviction waits for a task building the GPU batch")(
              write(broadcast))
            mayBuild.countDown()
            awaitOrFail(materializer, "the task did not build the GPU batch")
            withBroadcast(read(evicted))(assertSameData(expected, _))
            assertSameData(expected, broadcast)
          } finally {
            mayBuild.countDown()
            stop(pool)
          }
        }
      }
    }
  }

  test("reading an unknown broadcast stream format fails") {
    val baos = new ByteArrayOutputStream()
    withResource(new ObjectOutputStream(baos))(_.writeByte(Byte.MaxValue))
    assertThrows[IllegalStateException](read(baos.toByteArray).closeInternal())
  }

  test("reading a truncated packed broadcast fails") {
    val baos = new ByteArrayOutputStream()
    withResource(new ObjectOutputStream(baos)) { oos =>
      oos.writeByte(SerializeConcatHostBuffersDeserializeBatch.PACKED_FORMAT)
      oos.writeInt(1)
      oos.writeLong(100)
      oos.write(new Array[Byte](10))
    }
    val e = intercept[IllegalStateException](read(baos.toByteArray).closeInternal())
    assert(e.getMessage.contains("Expected to read 100 bytes, but got 10 bytes"))
  }
}
