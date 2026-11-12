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

import ai.rapids.cudf.{Rmm, RmmAllocationMode, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuDeviceManager, GpuSemaphore, NoopMetric,
  SpillableColumnarBatch, TestUtils}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.spill.{SpillFramework, SpillUnitTestBase}
import org.apache.commons.lang3.SerializationUtils

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Spark's `MemoryStore` serializes an evicted broadcast block while it holds the memory manager
 * lock, which GPU tasks may also need. Writing a broadcast that a task loaded onto the GPU must
 * therefore not wait for the GPU semaphore (https://github.com/NVIDIA/cudf-spark/issues/16460).
 * The broadcast keeps its host data after loading it onto the GPU, and writes that.
 */
class BroadcastEvictionSuite extends SpillUnitTestBase {
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

  /** Spill the GPU batch of `broadcast` to host memory, or further to disk. */
  private def spillGpuBatch(
      broadcast: SerializeConcatHostBuffersDeserializeBatch,
      toDisk: Boolean): Unit = {
    val size = broadcast.batch.sizeInBytes
    assert(SpillFramework.stores.deviceStore.spill(size) > 0)
    if (toDisk) {
      assert(SpillFramework.stores.hostStore.spill(size) > 0)
    }
  }

  Seq(("on the device", (1, 0, 0)), ("in host memory", (0, 1, 0)), ("on disk", (0, 0, 1))).foreach {
    case (location, expectedResidency) =>
      test(s"evicting a broadcast whose GPU batch is $location does not wait for the GPU") {
        withResource(buildBatch()) { expected =>
          withBroadcast(makeBroadcastBatch(expected)) { broadcast =>
            broadcast.batch
            if (expectedResidency._1 == 0) {
              spillGpuBatch(broadcast, toDisk = expectedResidency._3 == 1)
            }
            assert(residency == expectedResidency)
            // Evict it, then evict the reloaded copy before any task materializes it
            val (bytes, bytesAgain) = whileGpuIsBusy(evict(gpuDeadlock) {
              val bytes = write(broadcast)
              (bytes, withBroadcast(read(bytes))(write))
            })
            // Eviction leaves the GPU batch where it was
            assert(residency == expectedResidency)
            withBroadcast(read(bytes))(assertSameData(expected, _))
            withBroadcast(read(bytesAgain))(assertSameData(expected, _))
            assertSameData(expected, broadcast)
            // Evict a reloaded copy that a task materialized
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

  test("hostBatch of a materialized broadcast reads its host data") {
    withResource(buildBatch()) { expected =>
      withBroadcast(makeBroadcastBatch(expected)) { broadcast =>
        broadcast.batch
        spillGpuBatch(broadcast, toDisk = true)
        val actual = whileGpuIsBusy(evict("hostBatch waits for the GPU")(broadcast.hostBatch))
        // hostBatch leaves the GPU batch on disk
        assert(residency == ((0, 0, 1)))
        val expectedHost = new ColumnarBatch(
          GpuColumnVector.extractColumns(expected).safeMap(_.copyToHost(): ColumnVector),
          expected.numRows())
        withResource(Seq(expectedHost, actual)) { _ =>
          TestUtils.compareBatches(expectedHost, actual)
        }
      }
    }
  }

  test("evicting a broadcast does not wait for a task building its GPU batch") {
    withResource(buildBatch()) { expected =>
      val bytes = withBroadcast(makeBroadcastBatch(expected))(write)
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
