/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.spill

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.{Channels, FileChannel, WritableByteChannel}
import java.nio.file.StandardOpenOption
import java.util
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue

import scala.collection.mutable

import ai.rapids.cudf._
import com.nvidia.spark.rapids.{GpuColumnVector, GpuColumnVectorFromBuffer,
  GpuCompressedColumnVector, GpuDeviceManager, HashedPriorityQueue, HostAlloc,
  HostMemoryOutputStream, MemoryBufferToHostByteBufferIterator, NvtxId, NvtxRegistry,
  RapidsConf, RapidsHostColumnVector, TaskRegistryTracker}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableSeq
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.internal.HostByteBufferIterator
import com.nvidia.spark.rapids.jni.TaskPriority
import org.apache.commons.io.IOUtils

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.{GpuTaskMetrics, RapidsDiskBlockManager}
import org.apache.spark.sql.rapids.execution.{SerializedHostTableUtils, TrampolineUtil}
import org.apache.spark.sql.rapids.storage.RapidsStorageUtils
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.BlockId


/**
 * Spark-RAPIDS Spill Framework
 *
 * The spill framework tracks device/host/disk object lifecycle in the RAPIDS Accelerator
 * for Apache Spark. A set of stores is used to track these objects, which are wrapped in
 * "handles" that describe the state of each to the user and to the framework.
 *
 * This file comment covers some pieces of the framework that are worth knowing up front.
 *
 * Ownership:
 *
 * Any object handed to the framework via the factory methods for each of the handles
 * should not be used directly by the user. The framework takes ownership of all objects.
 * To get a reference back, call the `materialize` method, and always close what the framework
 * returns.
 *
 * CUDA/Host synchronization:
 *
 * We assume all device backed handles are completely materialized on the device (before adding
 * to the store, the CUDA stream has been synchronized with the CPU thread creating the handle),
 * and that all host memory backed handles are completely materialized and not mutated by
 * other CPU threads, because the contents of the handle may spill at any time, using any CUDA
 * stream or thread, without synchronization. If handles added to the store are not synchronized
 * we could write incomplete data to host memory or to disk.
 *
 * Spillability:
 *
 * An object is spillable (it will be copied to host or disk during OOM) if:
 * - it has a approxSizeInBytes > 0
 * - it is not actively being referenced by the user (call to `materialize`, or aliased)
 * - it hasn't already spilled, or is not currently being spilled
 * - it hasn't been closed
 *
 * Aliasing:
 *
 * We handle aliasing of objects, either in the spill framework or outside, by looking at the
 * reference count. All objects added to the store should support a ref count.
 * If the ref count is greater than the expected value, we assume it is being aliased,
 * and therefore we don't waste time spilling the aliased object. Please take a look at the
 * `spillable` method in each of the handles on how this is implemented.
 *
 * Materialization:
 *
 * Every store handle supports a `materialize` method that isn't part of the interface.
 * The reason is that to materialize certain objects, you may need some data (for example,
 * Spark schema descriptors). `materialize` incRefCounts the object if it's resident in the
 * intended store (`DeviceSpillableHandle` incRefCounts an object if it is in the device store),
 * and otherwise it will create a new copy from the spilled version and hand it to the user.
 * Any time a user calls `materialize`, they are responsible for closing the returned object.
 *
 * Spilling:
 *
 * A `SpillableHandle` will track an object in a specific store (`DeviceSpillable` tracks
 * device "intended" objects) for example. If the handle is asked to spill, it is the handle's
 * responsibility to initiate that spill, and to track the spilled handle (a device spillable
 * would have a `host` handle, which tracks the host spilled object).
 *
 * Spill is broken down into two methods: `spill` and `releaseSpilled`. This is a two stage
 * process because we need to make sure that there is no code running kernels on the spilled
 * data before we actually free it. See method documentations for `spill` and `releasedSpilled`
 * for more info.
 *
 * A cascade of spills can occur device -> host -> disk, given that host allocations can fail, or
 * could not fit in the SpillableHostStore's limit (if defined). In this case, the call to spill
 * will either create a host handle tracking an object on the host store (if we made room), or it
 * will create a host handle that points to a disk handle, tracking a file on disk.
 *
 * Host handles created directly, via the factory methods `SpillableHostBufferHandle(...)` or
 * `SpillableHostColumnarBatchHandle(...)`, do not trigger immediate spills. For example:
 * if the host store limit is set to 1GB, and we add a 1.5GB host buffer via
 * its factory method, we are going to have 2.5GB worth of host memory in the host store.
 * That said, if we run out of device memory and we need to spill to host, 1.5GB will be spilled
 * to disk, as device OOM triggers the pipeline spill.
 *
 * If we don't have a host store limit, spilling from the host store is done entirely via
 * host memory allocation failure callbacks. All objects added to the host store are tracked
 * immediately, since they were successfully allocated. If we fail to allocate host memory
 * during a device->host spill, however, the spill framework will bypass host memory and
 * go straight to disk (this last part works the same whether there are host limits or not).
 *
 * If the disk is full, we do not handle this in any special way. We expect this to be a
 * terminal state to the executor. Every handle spills to its own file on disk, identified
 * as a "temporary block" `BlockId` from Spark.
 *
 * Notes on locking:
 *
 * All stores use a concurrent hash map to store instances of `StoreHandle`. The only store
 * with extra locking is the `SpillableHostStore`, to maintain a `totalSize` number that is
 * used to figure out cheaply when it is full.
 *
 * All handles, except for disk handles, hold a reference to an object in their respective store:
 * `SpillableDeviceBufferHandle` has a `dev` reference that holds a `DeviceMemoryBuffer`, and a
 * `host` reference to `SpillableHostBufferHandle` that is only set if spilled. Disk handles are
 * different because they don't spill, as disk is considered the final store. When a user calls
 * `materialize` on a handle, the handle must guarantee that it can satisfy that, even if the caller
 * should wait until a spill happens. This is currently implemented using the handle lock.
 *
 * We never hold a store-wide coarse grain lock in the stores when we do IO.
 */

/**
 * Common interface for all handles in the spill framework.
 */
trait StoreHandle extends AutoCloseable {
  /**
   * Approximate size of this handle, used in three scenarios:
   * - Used by callers when accumulating up to a batch size for size goals.
   * - Used from the host store to figure out how much host memory total it is tracking.
   * - If approxSizeInBytes  is 0, the object is tracked by the stores so it can be
   *   removed on shutdown, or by handle.close, but 0-byte handles are not spillable.
   */
  val approxSizeInBytes: Long

  /**
   * This is used to resolve races between closing a handle and spilling.
   */
  private[spill] var closed: Boolean = false

  lazy val taskId: Option[Long] = Option(TaskContext.get()).map(tc => tc.taskAttemptId())

  /**
   * Be very careful that you set this before it is added into the Store, or that
   * the store knows to update the priority internally after this is set.
   */
  var taskPriority: Long = taskId.map(TaskPriority.getTaskPriority).getOrElse(Long.MaxValue)
}

trait SpillableHandle extends StoreHandle with Logging {
  /**
   * used to gate when a spill is actively being done so that a second thread won't
   * also begin spilling, and a handle won't release the underlying buffer if it's
   * closed while spilling
   */
  private[spill] var spilling: Boolean = false

  /**
   * Method called to spill this handle. It can be triggered from the spill store,
   * or directly against the handle.
   *
   * This will not free the spilled data. If you would like to free the spill
   * call `releaseSpilled`
   *
   * This is a thread-safe method. If multiple threads call it at the same time, one
   * thread will win and perform the spilling, and the other thread will make
   * no modification.
   *
   * If the disk is full, or a spill failure occurs otherwise (eg. device issues),
   * we make no attempt to handle it or restore state, as we expect to be in a non-recoverable
   * state at the task/executor level.
   *
   * @note The size returned from this method is only used by the spill framework
   *       to track the approximate size. It should just return `approxSizeInBytes`, as
   *       that's the size that it used when it first started tracking the object.
   * @return approxSizeInBytes if spilled, 0 for any other reason (not spillable, closed)
   */
  def spill(): Long

  /**
   * Method used to determine whether a handle tracks an object that could be spilled.
   * This is just a primary filtering mechanism, because there is a case where a handle
   * will appear spillable according to this check, but then a thread will not be able to
   * spill upon an attempt, because another thread has already started spilling the handle.
   * However, this is not expected to cause an issue, as it only would come up with multiple
   * threads trying to spill with overlapping spill plans. It would not, for instance,
   * produce any false negatives.
   *
   * @note At the level of `SpillableHandle`, the only requirement of spillability
   *       is that the size of the handle is > 0. `approxSizeInBytes` is known at
   *       construction, and is immutable.
   * @return true if currently spillable, false otherwise
   */
  private[spill] def spillable: Boolean = approxSizeInBytes > 0

  /**
   * Performs the actual closing of underlying buffers held by this handle, whereas
   * close is used to simply mark the state of the handle as closed. doClose will be
   * invoked within close if a thread is not currently spilling the handle, otherwise
   * the spill thread will be responsible for calling this after finishing its operation
   */
  private[spill] def doClose(): Unit

  /**
   * Executes a spill operation while handling exceptions.
   * If an exception occurs during spilling (e.g. InterruptedException), this method
   * catches it, logs a warning, resets the spilling state to false, and rethrows the exception.
   *
   * Subclasses override this method to wrap the spill operation with appropriate metrics tracking:
   * - DeviceSpillableHandle tracks spillToHostTime and memory bytes spilled
   * - HostSpillableHandle tracks spillToDiskTime and disk bytes spilled
   *
   * @param block the spill operation to execute, should return the number of bytes spilled
   * @return the number of bytes spilled, or 0 if the spill was not successful (only being used
   *         for metrics tracking for the time being)
   */
  protected def executeSpill(block: => Long): Long = {
    def impl(): Long = {
      try {
        block
      } catch {
        case e: Throwable =>
          logWarning(s"Failed to spill $this", e)
          synchronized {
            spilling = false
          }
          throw e
      }
    }
    impl()
  }

  /**
   * Marks the handle as closed, and closes the underlying buffers if the handle is not currently
   * spilling, otherwise the spill thread will do it.
   */
  override def close(): Unit = {
    val shouldClose = synchronized {
      if (closed) {
        // someone else already closed
        false
      } else {
        closed = true
        // if spilling, let the spilling thread doClose
        !spilling
      }
    }
    if (shouldClose) {
      doClose()
    }
  }

}

/**
 * Spillable handles that can be materialized on the device.
 * @tparam T an auto closeable subclass. `dev` tracks an instance of this object,
 *           on the device.
 */
trait DeviceSpillableHandle[T <: AutoCloseable] extends SpillableHandle {
  private[spill] var dev: Option[T]

  private[spill] override def spillable: Boolean = synchronized {
    super.spillable && dev.isDefined
  }

  protected def releaseDeviceResource(): Unit = {
    SpillFramework.removeFromDeviceStore(this)
    synchronized {
      dev.foreach(_.close())
      dev = None
    }
  }

  /**
   * Part two of the two-stage process for spilling device buffers. We call `releaseSpilled` after
   * a handle has spilled, and after a device synchronize. This prevents a race
   * between threads working on cuDF kernels, that did not synchronize while holding the
   * materialized handle's refCount, and the spiller thread (the spiller thread cannot
   * free a device buffer that the worker thread isn't done with).
   * See https://github.com/NVIDIA/spark-rapids/issues/8610 for more info.
   */
  def releaseSpilled(): Unit = {
    releaseDeviceResource()
  }

  // Wrap spill execution with memory spill metrics tracking
  override protected def executeSpill(block: => Long): Long = {
    GpuTaskMetrics.get.spillToHostTime {
      val spilledBytes = super.executeSpill(block)
      TrampolineUtil.incTaskMetricsMemoryBytesSpilled(spilledBytes)
      spilledBytes
    }
  }
}

/**
 * Spillable handles that can be materialized on the host.
 * @tparam T an auto closeable subclass. `host` tracks an instance of this object,
 *           on the host.
 */
trait HostSpillableHandle[T <: AutoCloseable] extends SpillableHandle {
  private[spill] var host: Option[T]

  private[spill] override def spillable: Boolean = synchronized {
    super.spillable && host.isDefined
  }

  protected def releaseHostResource(): Unit = {
    SpillFramework.removeFromHostStore(this)
    synchronized {
      host.foreach(_.close())
      host = None
    }
  }

  // Wrap spill execution with disk spill metrics tracking
  override protected def executeSpill(block: => Long): Long = {
    GpuTaskMetrics.get.spillToDiskTime {
      val spilledBytes = super.executeSpill(block)
      TrampolineUtil.incTaskMetricsDiskBytesSpilled(spilledBytes)
      spilledBytes
    }
  }
}

object SpillableHostBufferHandle extends Logging {
  def apply(hmb: HostMemoryBuffer): SpillableHostBufferHandle = {
    val handle = new SpillableHostBufferHandle(hmb.getLength, host = Some(hmb))
    SpillFramework.stores.hostStore.trackNoSpill(handle)
    handle
  }

  private[spill] def createHostHandleWithPacker(
      chunkedPacker: ChunkedPacker,
      taskPriority: Long): SpillableHostBufferHandle = {
    val handle = new SpillableHostBufferHandle(chunkedPacker.getTotalContiguousSize)
    handle.taskPriority = taskPriority
    withResource(
      SpillFramework.stores.hostStore.makeBuilder(handle)) { builder =>
      while (chunkedPacker.hasNext) {
        val (bb, len) = chunkedPacker.next()
        withResource(bb) { _ =>
          builder.copyNext(bb.buf, len, Cuda.DEFAULT_STREAM)
          // copyNext is synchronous w.r.t. the cuda stream passed,
          // no need to synchronize here.
        }
      }
      builder.build
    }
  }

  private[spill] def createHostHandleFromDeviceBuff(
      buff: DeviceMemoryBuffer,
      taskPriority: Long): SpillableHostBufferHandle = {
    val handle = new SpillableHostBufferHandle(buff.getLength)
    handle.taskPriority = taskPriority
    withResource(
      SpillFramework.stores.hostStore.makeBuilder(handle)) { builder =>
      builder.copyNext(buff, buff.getLength, Cuda.DEFAULT_STREAM)
      builder.build
    }
  }
}

class SpillableHostBufferHandle private (
    val sizeInBytes: Long,
    private[spill] override var host: Option[HostMemoryBuffer] = None,
    private[spill] var disk: Option[DiskHandle] = None)
  extends HostSpillableHandle[HostMemoryBuffer] {

  override val approxSizeInBytes: Long = sizeInBytes

  private[spill] override def spillable: Boolean = synchronized {
    if (super.spillable) {
      host.getOrElse {
        throw new IllegalStateException(
          s"$this is spillable but it doesn't have a materialized host buffer!")
      }.getRefCount == 1
    } else {
      false
    }
  }

  def materialize(): HostMemoryBuffer = {
    var materialized: HostMemoryBuffer = null
    var diskHandle: DiskHandle = null
    synchronized {
      if (closed) {
        throw new IllegalStateException(
          "attempting to materialize a closed handle")
      // after spilling, the host can get removed asynchronously, so let's
      // use disk if it's defined even if host is still present
      } else if (disk.isDefined) {
        diskHandle = disk.get
      } else if (host.isDefined) {
        materialized = host.get
        materialized.incRefCount()
      } else {
        throw new IllegalStateException(
          "open handle has no underlying buffer")
      }
    }
    if (materialized == null) {
      // Note that we are using a try finally here. This is to reduce the amount
      // of garbage collection that needs to happen. We could use a lot of
      // syntactic sugar to make the code look cleaner, but I don't want to
      // add more overhead for only a handful of places in the code.
      com.nvidia.spark.rapids.jni.RmmSpark.spillRangeStart()
      try {
        materialized = closeOnExcept(HostMemoryBuffer.allocate(sizeInBytes)) { hmb =>
          diskHandle.materializeToHostMemoryBuffer(hmb)
          hmb
        }
      } finally {
        com.nvidia.spark.rapids.jni.RmmSpark.spillRangeDone()
      }
    }
    materialized
  }

  override def spill(): Long = {
    if (!spillable) {
      0L
    } else {
      val thisThreadSpills = synchronized {
        if (!closed && disk.isEmpty && host.isDefined && !spilling) {
          spilling = true
          // incRefCount here so that if close() is called
          // while we are spilling, we will prevent the buffer being freed
          host.get.incRefCount()
          true
        } else {
          false
        }
      }
      if (thisThreadSpills) {
        executeSpill {
          withResource(host.get) { buf =>
            var staging: Option[DiskHandle] = None
            val actualBytes = withResource(DiskHandleStore.makeBuilder) { diskHandleBuilder =>
              val outputChannel = diskHandleBuilder.getChannel
              // the spill IO is non-blocking as it won't impact dev or host directly
              // instead we "atomically" swap the buffers below once they are ready
              val iter = new HostByteBufferIterator(buf)
              iter.foreach { bb =>
                try {
                  while (bb.hasRemaining) {
                    outputChannel.write(bb)
                  }
                } finally {
                  RapidsStorageUtils.dispose(bb)
                }
              }
              staging = Some(diskHandleBuilder.build(taskPriority))
              diskHandleBuilder.size // actual bytes to be spilled
            }
            // diskHandleBuilder is now closed, safe to expose disk handle to other threads
            synchronized {
              spilling = false
              if (closed) {
                staging.foreach(_.close())
                staging = None
                doClose()
              } else {
                disk = staging
              }
            }
            releaseHostResource()
            actualBytes // return actual bytes spilled for metrics
          }
        }
        sizeInBytes
      } else {
        0
      }
    }
  }

  override def doClose(): Unit = {
    releaseHostResource()
    disk.foreach(_.close())
    disk = None
  }

  private[spill] def materializeToDeviceMemoryBuffer(dmb: DeviceMemoryBuffer): Unit = {
    var hostBuffer: HostMemoryBuffer = null
    var diskHandle: DiskHandle = null
    synchronized {
      if (host.isDefined) {
        hostBuffer = host.get
        hostBuffer.incRefCount()
      } else if (disk.isDefined) {
        diskHandle = disk.get
      } else {
        throw new IllegalStateException(
          "attempting to materialize a closed handle")
      }
    }
    if (hostBuffer != null) {
      GpuTaskMetrics.get.readSpillFromHostTime {
        withResource(hostBuffer) { _ =>
          dmb.copyFromHostBuffer(
            /*dstOffset*/ 0,
            /*src*/ hostBuffer,
            /*srcOffset*/ 0,
            /*length*/ hostBuffer.getLength)
        }
      }
    } else {
      // cannot find a full host buffer, get chunked api
      // from disk
      diskHandle.materializeToDeviceMemoryBuffer(dmb)
    }
  }

  private[spill] def setHost(singleShotBuffer: HostMemoryBuffer): Unit = synchronized {
    host = Some(singleShotBuffer)
  }

  private[spill] def setDisk(handle: DiskHandle): Unit = synchronized {
    disk = Some(handle)
  }
}

object SpillableDeviceBufferHandle {
  def apply(dmb: DeviceMemoryBuffer): SpillableDeviceBufferHandle = {
    val handle = new SpillableDeviceBufferHandle(dmb.getLength, dev = Some(dmb))
    SpillFramework.stores.deviceStore.track(handle)
    handle
  }

  def apply(dmb: DeviceMemoryBuffer, overrideTaskPriority: Long): SpillableDeviceBufferHandle = {
    val handle = new SpillableDeviceBufferHandle(dmb.getLength, dev = Some(dmb))
    // Must be set before adding into the deviceStore
    handle.taskPriority = overrideTaskPriority
    SpillFramework.stores.deviceStore.track(handle)
    handle
  }
}

class SpillableDeviceBufferHandle private (
    val sizeInBytes: Long,
    private[spill] override var dev: Option[DeviceMemoryBuffer],
    private[spill] var host: Option[SpillableHostBufferHandle] = None)
    extends DeviceSpillableHandle[DeviceMemoryBuffer] {

  override val approxSizeInBytes: Long = sizeInBytes

  private[spill] override def spillable: Boolean = synchronized {
    if (super.spillable) {
      dev.getOrElse {
        throw new IllegalStateException(
          s"$this is spillable but it doesn't have a dev buffer!")
      }.getRefCount == 1
    } else {
      false
    }
  }

  def materialize(): DeviceMemoryBuffer = {
    var materialized: DeviceMemoryBuffer = null
    var hostHandle: SpillableHostBufferHandle = null
    synchronized {
      if (closed) {
        throw new IllegalStateException(
          "attempting to materialize a closed handle")
      } else if (host.isDefined) {
        // since we spilled, host must be set.
        hostHandle = host.get
      } else if (dev.isDefined) {
        materialized = dev.get
        materialized.incRefCount()
      } else {
        throw new IllegalStateException(
          "open handle has no underlying buffer")
      }
    }
    // if `materialized` is null, we spilled. This is a terminal
    // state, as we are not allowing unspill, and we don't need
    // to hold locks while we copy back from here.
    if (materialized == null) {
      // Note that we are using a try finally here. This is to reduce the amount
      // of garbage collection that needs to happen. We could use a lot of
      // syntactic sugar to make the code look cleaner, but I don't want to
      // add more overhead for only a handful of places in the code.
      com.nvidia.spark.rapids.jni.RmmSpark.spillRangeStart()
      try {
        materialized = closeOnExcept(DeviceMemoryBuffer.allocate(sizeInBytes)) { dmb =>
          hostHandle.materializeToDeviceMemoryBuffer(dmb)
          dmb
        }
      } finally {
        com.nvidia.spark.rapids.jni.RmmSpark.spillRangeDone()
      }
    }
    materialized
  }

  override def spill(): Long = {
    if (!spillable) {
      0L
    } else {
      val thisThreadSpills = synchronized {
        if (!closed && host.isEmpty && dev.isDefined && !spilling) {
          spilling = true
          // incRefCount here so that if close() is called
          // while we are spilling, we will prevent the buffer being freed
          dev.get.incRefCount()
          true
        } else {
          false
        }
      }
      if (thisThreadSpills) {
        executeSpill {
          withResource(dev.get) { buf =>
            // the spill IO is non-blocking as it won't impact dev or host directly
            // instead we "atomically" swap the buffers below once they are ready
            var stagingHost: Option[SpillableHostBufferHandle] =
              Some(SpillableHostBufferHandle.createHostHandleFromDeviceBuff(buf, taskPriority))
            synchronized {
              spilling = false
              if (closed) {
                stagingHost.foreach(_.close())
                stagingHost = None
                doClose()
              } else {
                host = stagingHost
              }
            }
            buf.getLength // return actual bytes spilled for metrics
          }
        }
        sizeInBytes
      } else {
        0
      }
    }
  }

  override def doClose(): Unit = {
    releaseDeviceResource()
    host.foreach(_.close())
    host = None
  }
}

class SpillableColumnarBatchHandle private (
    override val approxSizeInBytes: Long,
    private[spill] override var dev: Option[ColumnarBatch],
    private[spill] var host: Option[SpillableHostBufferHandle] = None)
  extends DeviceSpillableHandle[ColumnarBatch] with Logging {

  override def spillable: Boolean = synchronized {
    if (super.spillable) {
      val dcvs = GpuColumnVector.extractBases(dev.get)
      val colRepetition = mutable.HashMap[ColumnVector, Int]()
      dcvs.foreach { hcv =>
        colRepetition.put(hcv, colRepetition.getOrElse(hcv, 0) + 1)
      }
      dcvs.forall(dcv => {
        colRepetition(dcv) == dcv.getRefCount
      })
    } else {
      false
    }
  }

  private var meta: Option[ByteBuffer] = None

  def materialize(dt: Array[DataType]): ColumnarBatch = {
    var materialized: ColumnarBatch = null
    var hostHandle: SpillableHostBufferHandle = null
    synchronized {
      if (closed) {
        throw new IllegalStateException(
          "attempting to materialize a closed handle")
      } else if (host.isDefined) {
        hostHandle = host.get
      } else if (dev.isDefined) {
        materialized = GpuColumnVector.incRefCounts(dev.get)
      } else {
        throw new IllegalStateException(
          "open handle has no underlying buffer")
      }
    }
    if (materialized == null) {
      // Note that we are using a try finally here. This is to reduce the amount
      // of garbage collection that needs to happen. We could use a lot of
      // syntactic sugar to make the code look cleaner, but I don't want to
      // add more overhead for only a handful of places in the code.
      com.nvidia.spark.rapids.jni.RmmSpark.spillRangeStart()
      try {
        val devBuffer = closeOnExcept(DeviceMemoryBuffer.allocate(hostHandle.sizeInBytes)) { dmb =>
          hostHandle.materializeToDeviceMemoryBuffer(dmb)
          dmb
        }
        val cb = withResource(devBuffer) { _ =>
          withResource(Table.fromPackedTable(meta.get, devBuffer)) { tbl =>
            GpuColumnVector.from(tbl, dt)
          }
        }
        materialized = cb
      } finally {
        com.nvidia.spark.rapids.jni.RmmSpark.spillRangeDone()
      }
    }
    materialized
  }

  override def spill(): Long = {
    if (!spillable) {
      0L
    } else {
      val thisThreadSpills = synchronized {
        if (!closed && host.isEmpty && dev.isDefined && !spilling) {
          spilling = true
          GpuColumnVector.incRefCounts(dev.get)
          true
        } else {
          false
        }
      }
      if (thisThreadSpills) {
        executeSpill {
          withChunkedPacker(dev.get) { chunkedPacker =>
            meta = Some(chunkedPacker.getPackedMeta)
            var staging: Option[SpillableHostBufferHandle] =
              Some(SpillableHostBufferHandle.createHostHandleWithPacker(chunkedPacker,
                taskPriority))
            val actualBytes: Long = staging.map(_.sizeInBytes).getOrElse(0L)
            synchronized {
              spilling = false
              if (closed) {
                staging.foreach(_.close())
                staging = None
                doClose()
              } else {
                host = staging
              }
            }
            actualBytes // return actual bytes spilled for metrics
          }
        }
        // We return the size we were created with. This is not the actual size
        // of this batch when it is packed, and it is used by the calling code
        // to figure out more or less how much did we free in the device.
        approxSizeInBytes
      } else {
        0L
      }
    }
  }

  private def withChunkedPacker[T](batchToPack: ColumnarBatch)(body: ChunkedPacker => T): T = {
    val tbl = withResource(batchToPack) { _ =>
      GpuColumnVector.from(batchToPack)
    }
    withResource(tbl) { _ =>
      withResource(new ChunkedPacker(tbl, SpillFramework.chunkedPackBounceBufferPool)) { packer =>
        body(packer)
      }
    }
  }

  override def doClose(): Unit = {
    releaseDeviceResource()
    host.foreach(_.close())
    host = None
  }
}

object SpillableColumnarBatchFromBufferHandle {
  def apply(
      ct: ContiguousTable,
      dataTypes: Array[DataType]): SpillableColumnarBatchFromBufferHandle = {
    withResource(ct) { _ =>
      val sizeInBytes = ct.getBuffer.getLength
      val cb = GpuColumnVectorFromBuffer.from(ct, dataTypes)
      val handle = new SpillableColumnarBatchFromBufferHandle(
        sizeInBytes, dev = Some(cb))
      SpillFramework.stores.deviceStore.track(handle)
      handle
    }
  }

  def apply(cb: ColumnarBatch): SpillableColumnarBatchFromBufferHandle = {
    require(GpuColumnVectorFromBuffer.isFromBuffer(cb),
      "Columnar batch isn't a batch from buffer")
    val sizeInBytes =
      cb.column(0).asInstanceOf[GpuColumnVectorFromBuffer].getBuffer.getLength
    val handle = new SpillableColumnarBatchFromBufferHandle(
      sizeInBytes, dev = Some(cb))
    SpillFramework.stores.deviceStore.track(handle)
    handle
  }
}

class SpillableColumnarBatchFromBufferHandle private (
    val sizeInBytes: Long,
    private[spill] override var dev: Option[ColumnarBatch],
    private[spill] var host: Option[SpillableHostBufferHandle] = None)
  extends DeviceSpillableHandle[ColumnarBatch] {

  override val approxSizeInBytes: Long = sizeInBytes

  private var meta: Option[TableMeta] = None

  private[spill] override def spillable: Boolean = synchronized {
    if (super.spillable) {
      val dcvs = GpuColumnVector.extractBases(dev.get)
      val colRepetition = mutable.HashMap[ColumnVector, Int]()
      dcvs.foreach { hcv =>
        colRepetition.put(hcv, colRepetition.getOrElse(hcv, 0) + 1)
      }
      dcvs.forall(dcv => {
        colRepetition(dcv) == dcv.getRefCount
      })
    } else {
      false
    }
  }

  def materialize(dt: Array[DataType]): ColumnarBatch = {
    var materialized: ColumnarBatch = null
    var hostHandle: SpillableHostBufferHandle = null
    synchronized {
      if (closed) {
        throw new IllegalStateException(
          "attempting to materialize a closed handle")
      } else if (host.isDefined) {
        hostHandle = host.get
      } else if (dev.isDefined) {
        materialized = GpuColumnVector.incRefCounts(dev.get)
      } else {
        throw new IllegalStateException(
          "open handle has no underlying buffer")
      }
    }
    if (materialized == null) {
      // Note that we are using a try finally here. This is to reduce the amount
      // of garbage collection that needs to happen. We could use a lot of
      // syntactic sugar to make the code look cleaner, but I don't want to
      // add more overhead for only a handful of places in the code.
      com.nvidia.spark.rapids.jni.RmmSpark.spillRangeStart()
      try {
        val devBuffer = closeOnExcept(DeviceMemoryBuffer.allocate(hostHandle.sizeInBytes)) { dmb =>
          hostHandle.materializeToDeviceMemoryBuffer(dmb)
          dmb
        }
        val cb = withResource(devBuffer) { _ =>
          withResource(Table.fromPackedTable(meta.get.packedMetaAsByteBuffer(), devBuffer)) { tbl =>
            GpuColumnVector.from(tbl, dt)
          }
        }
        materialized = cb
      } finally {
        com.nvidia.spark.rapids.jni.RmmSpark.spillRangeDone()
      }
    }
    materialized
  }

  override def spill(): Long = {
    if (!spillable) {
      0
    } else {
      val thisThreadSpills = synchronized {
        if (!closed && host.isEmpty && dev.isDefined && !spilling) {
          spilling = true
          GpuColumnVector.incRefCounts(dev.get)
          true
        } else {
          false
        }
      }
      if (thisThreadSpills) {
        executeSpill {
          withResource(dev.get) { cb =>
            val cvFromBuffer = cb.column(0).asInstanceOf[GpuColumnVectorFromBuffer]
            meta = Some(cvFromBuffer.getTableMeta)
            var staging: Option[SpillableHostBufferHandle] =
              Some(SpillableHostBufferHandle.createHostHandleFromDeviceBuff(
                cvFromBuffer.getBuffer, taskPriority))
            synchronized {
              spilling = false
              if (closed) {
                doClose()
                staging.foreach(_.close())
                staging = None
              } else {
                host = staging
              }
            }
            cvFromBuffer.getBuffer.getLength // return actual bytes spilled for metrics
          }
        }
        sizeInBytes
      } else {
        0L
      }
    }
  }

  override def doClose(): Unit = {
    releaseDeviceResource()
    host.foreach(_.close())
    host = None
  }
}

object SpillableCompressedColumnarBatchHandle {
  def apply(cb: ColumnarBatch): SpillableCompressedColumnarBatchHandle = {
    require(GpuCompressedColumnVector.isBatchCompressed(cb),
      "Tried to track a compressed batch, but the batch wasn't compressed")
    val compressedSize =
      cb.column(0).asInstanceOf[GpuCompressedColumnVector].getTableBuffer.getLength
    val handle = new SpillableCompressedColumnarBatchHandle(compressedSize, dev = Some(cb))
    SpillFramework.stores.deviceStore.track(handle)
    handle
  }
}

class SpillableCompressedColumnarBatchHandle private (
    val compressedSizeInBytes: Long,
    private[spill] override var dev: Option[ColumnarBatch],
    private[spill] var host: Option[SpillableHostBufferHandle] = None)
  extends DeviceSpillableHandle[ColumnarBatch] {

  override val approxSizeInBytes: Long = compressedSizeInBytes

  protected var meta: Option[TableMeta] = None

  override def spillable: Boolean = synchronized {
    if (super.spillable) {
      val cb = dev.get
      val buff = cb.column(0).asInstanceOf[GpuCompressedColumnVector].getTableBuffer
      buff.getRefCount == 1
    } else {
      false
    }
  }

  def materialize(): ColumnarBatch = {
    var materialized: ColumnarBatch = null
    var hostHandle: SpillableHostBufferHandle = null
    synchronized {
      if (closed) {
        throw new IllegalStateException(
          "attempting to materialize a closed handle")
      } else if (host.isDefined) {
        hostHandle = host.get
      } else if (dev.isDefined) {
        materialized = GpuCompressedColumnVector.incRefCounts(dev.get)
      } else {
        throw new IllegalStateException(
          "open handle has no underlying buffer")
      }
    }
    if (materialized == null) {
      // Note that we are using a try finally here. This is to reduce the amount
      // of garbage collection that needs to happen. We could use a lot of
      // syntactic sugar to make the code look cleaner, but I don't want to
      // add more overhead for only a handful of places in the code.
      com.nvidia.spark.rapids.jni.RmmSpark.spillRangeStart()
      try {
        val devBuffer = closeOnExcept(DeviceMemoryBuffer.allocate(hostHandle.sizeInBytes)) { dmb =>
          hostHandle.materializeToDeviceMemoryBuffer(dmb)
          dmb
        }
        materialized = withResource(devBuffer) { _ =>
          GpuCompressedColumnVector.from(devBuffer, meta.get)
        }
      } finally {
        com.nvidia.spark.rapids.jni.RmmSpark.spillRangeDone()
      }
    }
    materialized
  }

  override def spill(): Long = {
    if (!spillable) {
      0L
    } else {
      val thisThreadSpills = synchronized {
        if (!closed && host.isEmpty && dev.isDefined && !spilling) {
          spilling = true
          GpuCompressedColumnVector.incRefCounts(dev.get)
          true
        } else {
          false
        }
      }
      if (thisThreadSpills) {
        executeSpill {
          withResource(dev.get) { cb =>
            val cvFromBuffer = cb.column(0).asInstanceOf[GpuCompressedColumnVector]
            meta = Some(cvFromBuffer.getTableMeta)
            var staging: Option[SpillableHostBufferHandle] =
              Some(SpillableHostBufferHandle.createHostHandleFromDeviceBuff(
                cvFromBuffer.getTableBuffer, taskPriority))
            synchronized {
              spilling = false
              if (closed) {
                doClose()
                staging = None
              } else {
                host = staging
              }
            }
            cvFromBuffer.getTableBuffer.getLength // return actual bytes spilled for metrics
          }
        }
        compressedSizeInBytes
      } else {
        0L
      }
    }
  }

  override def doClose(): Unit = {
    releaseDeviceResource()
    host.foreach(_.close())
    host = None
    meta = None
  }
}

object SpillableHostColumnarBatchHandle {
  def apply(cb: ColumnarBatch): SpillableHostColumnarBatchHandle = {
    val sizeInBytes = RapidsHostColumnVector.getTotalHostMemoryUsed(cb)
    val handle = new SpillableHostColumnarBatchHandle(sizeInBytes, cb.numRows(), host = Some(cb))
    SpillFramework.stores.hostStore.trackNoSpill(handle)
    handle
  }
}

class SpillableHostColumnarBatchHandle private (
    override val approxSizeInBytes: Long,
    val numRows: Int,
    private[spill] override var host: Option[ColumnarBatch],
    private[spill] var disk: Option[DiskHandle] = None)
  extends HostSpillableHandle[ColumnarBatch] {

  override def spillable: Boolean = synchronized {
    if (super.spillable) {
      val hcvs = RapidsHostColumnVector.extractBases(host.get)
      val colRepetition = mutable.HashMap[HostColumnVector, Int]()
      hcvs.foreach { hcv =>
        colRepetition.put(hcv, colRepetition.getOrElse(hcv, 0) + 1)
      }
      hcvs.forall(hcv => {
        colRepetition(hcv) == hcv.getRefCount
      })
    } else {
      false
    }
  }

  def materialize(sparkTypes: Array[DataType]): ColumnarBatch = {
    var materialized: ColumnarBatch = null
    var diskHandle: DiskHandle = null
    synchronized {
      if (closed) {
        throw new IllegalStateException(
          "attempting to materialize a closed handle")
      // after spilling, the host can get removed asynchronously, so let's
      // use disk if it's defined even if host is still present
      } else if (disk.isDefined) {
        diskHandle = disk.get
      } else if (host.isDefined) {
        materialized = RapidsHostColumnVector.incRefCounts(host.get)
      } else {
        throw new IllegalStateException(
          "open handle has no underlying buffer")
      }
    }
    if (materialized == null) {
      // Note that we are using a try finally here. This is to reduce the amount
      // of garbage collection that needs to happen. We could use a lot of
      // syntactic sugar to make the code look cleaner, but I don't want to
      // add more overhead for only a handful of places in the code.
      com.nvidia.spark.rapids.jni.RmmSpark.spillRangeStart()
      try {
        materialized = diskHandle.withInputWrappedStream { inputStream =>
          val (header, hostBuffer) = SerializedHostTableUtils.readTableHeaderAndBuffer(inputStream)
          val hostCols = withResource(hostBuffer) { _ =>
            SerializedHostTableUtils.buildHostColumns(header, hostBuffer, sparkTypes)
          }
          new ColumnarBatch(hostCols.toArray, numRows)
        }
      } finally {
        com.nvidia.spark.rapids.jni.RmmSpark.spillRangeDone()
      }
    }
    materialized
  }

  override def spill(): Long = {
    if (!spillable) {
      0L
    } else {
      val thisThreadSpills = synchronized {
        if (!closed && disk.isEmpty && host.isDefined && !spilling) {
          spilling = true
          RapidsHostColumnVector.incRefCounts(host.get)
          true
        } else {
          false
        }
      }
      if (thisThreadSpills) {
        executeSpill {
          withResource(host.get) { cb =>
            withResource(DiskHandleStore.makeBuilder) { diskHandleBuilder =>
              val dos = diskHandleBuilder.getDataOutputStream
              val columns = RapidsHostColumnVector.extractBases(cb)
              JCudfSerialization.writeToStream(columns, dos, 0, cb.numRows())
              val actualBytes = diskHandleBuilder.size
              var staging: Option[DiskHandle] = Some(diskHandleBuilder.build(taskPriority))
              synchronized {
                spilling = false
                if (closed) {
                  doClose()
                  staging.foreach(_.close())
                  staging = None
                } else {
                  disk = staging
                }
              }
              releaseHostResource()
              actualBytes // return actual bytes spilled for metrics
            }
          }
        }
        approxSizeInBytes
      } else {
        0L
      }
    }
  }

  override def doClose(): Unit = {
    releaseHostResource()
    disk.foreach(_.close())
    disk = None
  }
}

object DiskHandle {
  def apply(blockId: BlockId,
            offset: Long,
            diskSizeInBytes: Long): DiskHandle = {
    val handle = new DiskHandle(
      blockId, offset, diskSizeInBytes)
    SpillFramework.stores.diskStore.track(handle)
    handle
  }

  def apply(blockId: BlockId,
            offset: Long,
            diskSizeInBytes: Long,
            taskPriority: Long): DiskHandle = {
    val handle = new DiskHandle(
      blockId, offset, diskSizeInBytes)
    handle.taskPriority = taskPriority
    SpillFramework.stores.diskStore.track(handle)
    handle
  }
}

/**
 * A disk buffer handle helps us track spill-framework originated data on disk.
 * This type of handle isn't spillable, and therefore it just implements `StoreHandle`
 * @param blockId - a spark `BlockId` obtained from the configured `BlockManager`
 * @param offset - starting offset for the data within the file backing `blockId`
 * @param sizeInBytes - amount of bytes on disk (usually compressed and could also be encrypted).
 */
class DiskHandle private(
    val blockId: BlockId,
    val offset: Long,
    val sizeInBytes: Long)
  extends StoreHandle {

  override val approxSizeInBytes: Long = sizeInBytes

  private def withInputChannel[T](body: FileChannel => T): T = synchronized {
    val file = SpillFramework.stores.diskStore.diskBlockManager.getFile(blockId)
    withResource(new FileInputStream(file)) { fs =>
      withResource(fs.getChannel) { channel =>
        body(channel)
      }
    }
  }

  def withInputWrappedStream[T](body: InputStream => T): T = synchronized {
    val diskBlockManager = SpillFramework.stores.diskStore.diskBlockManager
    val serializerManager = diskBlockManager.getSerializerManager()
    GpuTaskMetrics.get.readSpillFromDiskTime {
      withInputChannel { inputChannel =>
        inputChannel.position(offset)
        withResource(Channels.newInputStream(inputChannel)) { compressed =>
          withResource(serializerManager.wrapStream(blockId, compressed)) { in =>
            body(in)
          }
        }
      }
    }
  }

  override def close(): Unit = {
    SpillFramework.removeFromDiskStore(this)
    SpillFramework.stores.diskStore.deleteFile(blockId)
  }

  /**
   * Materialize the data from disk to the given host memory buffer.
   *
   * @param mb the host memory buffer to copy data into. The buffer's capacity
   *           must be exactly equal to the decompressed data length. Note that this
   *           may differ from `sizeInBytes` since data on disk may be compressed.
   * @throws IllegalStateException if the actual bytes read from disk does not match
   *                               the buffer's length
   */
  def materializeToHostMemoryBuffer(mb: HostMemoryBuffer): Unit = {
    withInputWrappedStream { in =>
      withResource(new HostMemoryOutputStream(mb)) { out =>
        val len = IOUtils.copy(in, out)
        if (len != mb.getLength) {
          throw new IllegalStateException(
            s"Expected to read ${mb.getLength} bytes, but got $len bytes from disk")
        }
      }
    }
  }

  /**
   * Materialize the data from disk to the given device memory buffer.
   *
   * @param dmb the device memory buffer to copy data into. The buffer's capacity
   *            must be exactly equal to the decompressed data length. Note that this
   *            may differ from `sizeInBytes` since data on disk may be compressed.
   * @throws IllegalStateException if the actual bytes read from disk does not match
   *                               the buffer's length
   */
  def materializeToDeviceMemoryBuffer(dmb: DeviceMemoryBuffer): Unit = {
    var copyOffset = 0L
    withInputWrappedStream { in =>
      SpillFramework.withHostSpillBounceBuffer { hmb =>
        val bbLength = hmb.getLength.toInt
        withResource(new HostMemoryOutputStream(hmb)) { out =>
          var sizeRead = IOUtils.copyLarge(in, out, 0, bbLength)
          while (sizeRead > 0) {
            // this syncs at every copy, since for now we are
            // reusing a single host spill bounce buffer
            dmb.copyFromHostBuffer(
              /*dstOffset*/ copyOffset,
              /*src*/ hmb,
              /*srcOffset*/ 0,
              /*length*/ sizeRead)
            out.seek(0) // start over
            copyOffset += sizeRead
            sizeRead = IOUtils.copyLarge(in, out, 0, bbLength)
          }
        }
      }
    }
    if (copyOffset != dmb.getLength) {
      throw new IllegalStateException(
        s"Expected to read ${dmb.getLength} bytes, but got $copyOffset bytes from disk")
    }
  }
}

object HandleComparator extends util.Comparator[StoreHandle] {

  override def compare(t1: StoreHandle, t2: StoreHandle): Int = {
    java.lang.Long.compare(t1.taskPriority, t2.taskPriority)
  }
}

trait HandleStore[T <: StoreHandle] extends AutoCloseable with Logging {
  protected lazy val handles = new HashedPriorityQueue[T](HandleComparator)

  def numHandles: Int = synchronized {
    handles.size()
  }

  def track(handle: T): Unit = synchronized {
    doTrack(handle)
  }

  def remove(handle: T): Unit = synchronized {
    doRemove(handle)
  }

  def isEmpty: Boolean = synchronized {
    handles.isEmpty
  }

  protected def doTrack(handle: T): Boolean = synchronized {
    if (handles.contains(handle)) {
      false
    } else {
      handles.offer(handle)
      true
    }
  }

  protected def doRemove(handle: T): Boolean = synchronized {
    handles.remove(handle)
  }

  /**
   * Close all handles in the store.
   *
   * Lock ordering: To avoid AB-BA deadlock between store lock and handle lock,
   * we copy the handles list under the store lock, then close handles outside
   * the lock. This ensures we always acquire handle locks without holding the
   * store lock, matching the lock order in handle.spill() which acquires
   * handle lock first, then store lock via removeFromXxxStore().
   */
  override def close(): Unit = {
    val handlesToClose = synchronized {
      val list = new util.ArrayList[T](handles)
      handles.clear()
      list
    }
    // Close handles outside the store lock to avoid deadlock
    handlesToClose.forEach(_.close())
  }
}

trait SpillableStore[T <: SpillableHandle]
    extends HandleStore[T] with Logging {
  protected def spillNvtxRange: NvtxId

  /**
   * Internal class to provide an interface to our plan for this spill.
   *
   * We will build up this SpillPlan by adding spillables: handles
   * that are marked spillable given the `spillable` method returning true.
   * The spill store will call `trySpill`, which moves handles from the
   * `spillableHandles` array to the `spilledHandles` array.
   *
   * At any point in time, a spill framework can call `getSpilled`
   * to obtain the list of spilled handles. The device store does this
   * to inject CUDA synchronization before actually releasing device handles.
   */
  class SpillPlan {
    private val spillableHandles = new util.ArrayList[T]()
    private val spilledHandles = new util.ArrayList[T]()

    def add(spillable: T): Unit = {
      spillableHandles.add(spillable)
    }

    def trySpill(): Long = {
      var amountSpilled = 0L
      val it = spillableHandles.iterator()
      while (it.hasNext) {
        val handle = it.next()
        val spilled = handle.spill()
        if (spilled > 0) {
          // this thread was successful at spilling handle.
          amountSpilled += spilled
          spilledHandles.add(handle)
        } else {
          // else, either:
          // - this thread lost the race and the handle was closed
          // - another thread spilled it
          // - the handle isn't spillable anymore, due to ref count.
          it.remove()
        }
      }
      amountSpilled
    }

    def getSpilled: util.ArrayList[T] = {
      spilledHandles
    }
  }

  private def makeSpillPlan(spillNeeded: Long): SpillPlan = {
    val plan = new SpillPlan()
    var amountToSpill = 0L
    val allHandles = synchronized {
      handles.priorityIterator()
    }
    // two threads could be here trying to spill and creating a list of spillables
    while (allHandles.hasNext && amountToSpill < spillNeeded) {
      val handle = allHandles.next()
      if (handle.spillable) {
        amountToSpill += handle.approxSizeInBytes
        plan.add(handle)
      }
    }
    plan
  }

  protected def postSpill(plan: SpillPlan): Unit = {}

  def spill(spillNeeded: Long): Long = {
    if (spillNeeded == 0) {
      0L
    } else {
      spillNvtxRange {
        // Note that we are using a try finally here. This is to reduce the amount
        // of garbage collection that needs to happen. We could use a lot of
        // syntactic sugar to make the code look cleaner, but I don't want to
        // add more overhead for only a handful of places in the code.
        com.nvidia.spark.rapids.jni.RmmSpark.spillRangeStart()
        try {
          val plan = makeSpillPlan(spillNeeded)
          val amountSpilled = plan.trySpill()
          postSpill(plan)
          amountSpilled
        } finally {
          com.nvidia.spark.rapids.jni.RmmSpark.spillRangeDone()
        }
      }
    }
  }

  /**
   * Get a summary of spillable handles in the store.
   *
   * Lock ordering: To avoid AB-BA deadlock between store lock and handle lock,
   * we copy the handles list under the store lock, then check spillable status
   * outside the lock. The handle.spillable check acquires the handle lock,
   * so we must not hold the store lock while calling it.
   */
  def spillableSummary(): String = {
    val handlesCopy = synchronized {
      new util.ArrayList[T](handles)
    }
    var spillableHandleCount = 0L
    var spillableHandleBytes = 0L
    var totalHandleBytes = 0L
    // Iterate outside the store lock to avoid deadlock
    handlesCopy.forEach(handle => {
      totalHandleBytes += handle.approxSizeInBytes
      if (handle.spillable) {
        spillableHandleCount += 1
        spillableHandleBytes += handle.approxSizeInBytes
      }
    })
    s"SpillableStore: ${this.getClass.getSimpleName}, " +
      s"Total Handles: $numHandles, " +
      s"Spillable Handles: $spillableHandleCount, " +
      s"Total Handle Bytes: $totalHandleBytes, " +
      s"Spillable Handle Bytes: $spillableHandleBytes"
  }
}

class SpillableHostStore(val maxSize: Option[Long] = None)
  extends SpillableStore[HostSpillableHandle[_]]
    with Logging {

  private[spill] var totalSize: Long = 0L

  private def tryTrack(handle: HostSpillableHandle[_]): Boolean = {
    if (maxSize.isEmpty || handle.approxSizeInBytes == 0) {
      super.doTrack(handle)
      // for now, keep this totalSize part, we technically
      // do not need to track `totalSize` if we don't have a limit
      synchronized {
        totalSize += handle.approxSizeInBytes
      }
      true
    } else {
      synchronized {
        val storeMaxSize = maxSize.get
        if (totalSize > 0 && totalSize + handle.approxSizeInBytes > storeMaxSize) {
          // we want to try to make room for this buffer
          false
        } else {
          // it fits
          if (super.doTrack(handle)) {
            totalSize += handle.approxSizeInBytes
          }
          true
        }
      }
    }
  }

  override def track(handle: HostSpillableHandle[_]): Unit = {
    trackInternal(handle)
  }

  private def trackInternal(handle: HostSpillableHandle[_]): Boolean = {
    // try to track the handle: in the case of no limits
    // this should just be add to the store
    var tracked = false
    tracked = tryTrack(handle)
    if (!tracked) {
      // we only end up here if we have host store limits.
      var numRetries = 0
      // we are going to try to track again, in a loop,
      // since we want to release
      var canFit = true
      val handleSize = handle.approxSizeInBytes
      var amountSpilled = 0L
      val hadHandlesToSpill = !isEmpty
      while (canFit && !tracked && numRetries < 5) {
        // if we are trying to add a handle larger than our limit
        if (maxSize.get < handleSize) {
          // no point in checking how much is free, just spill all
          // we have
          amountSpilled += spill(maxSize.get)
        } else {
          // handleSize is within the limits
          val freeAmount = synchronized {
            maxSize.get - totalSize
          }
          val spillNeeded = handleSize - freeAmount
          if (spillNeeded > 0) {
            amountSpilled += spill(spillNeeded)
          }
        }
        tracked = tryTrack(handle)
        if (!tracked) {
          // we tried to spill, and we still couldn't fit this buffer
          // if we have a totalSize > 0, we could try some more
          // the disk api
          synchronized {
            canFit = totalSize > 0
          }
        }
        numRetries += 1
      }
      val taskId = Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(0)
      if (hadHandlesToSpill) {
        logInfo(s"Task $taskId spilled $amountSpilled bytes while trying to " +
          s"track $handleSize bytes.")
      }
    }
    tracked
  }

  /**
   * This is a special method in the host store where spillable handles can be added
   * but they will not trigger the cascade host->disk spill logic. This is to replicate
   * how the stores used to work in the past, and is only called from factory
   * methods that are used by client code.
   */
  def trackNoSpill(handle: HostSpillableHandle[_]): Unit = {
    synchronized {
      if (doTrack(handle)) {
        totalSize += handle.approxSizeInBytes
      }
    }
  }

  override def remove(handle: HostSpillableHandle[_]): Unit = {
    synchronized {
      if (doRemove(handle)) {
        totalSize -= handle.approxSizeInBytes
      }
    }
  }

  /**
   * Makes a builder object for `SpillableHostBufferHandle`. The builder will
   * either copy ot host or disk, if the host buffer fits in the host store (if tracking
   * is enabled).
   *
   * Host store locks and disk store locks will be taken/released during this call, but
   * after the builder is created, no locks are held in the store.
   *
   * @note When creating the host buffer handle, never call the factory Spillable* methods,
   *       instead, construct the handles directly. This is because the factory methods
   *       trigger a spill to disk, and that standard behavior of the spill framework so far.
   * @param handle a host handle that only has a size set, and no backing store.
   * @return the builder to be closed by caller
   */
  def makeBuilder(handle: SpillableHostBufferHandle): SpillableHostBufferHandleBuilder = {
    var builder: Option[SpillableHostBufferHandleBuilder] = None
    if (handle.sizeInBytes <= maxSize.getOrElse(Long.MaxValue)) {
      HostAlloc.tryAlloc(handle.sizeInBytes).foreach { hmb =>
        withResource(hmb) { _ =>
          if (trackInternal(handle)) {
            hmb.incRefCount()
            // the host store made room or fit this buffer
            builder = Some(new SpillableHostBufferHandleBuilderForHost(handle, hmb))
          }
        }
      }
    }
    builder.getOrElse {
      // the disk store will track this when we call .build
      new SpillableHostBufferHandleBuilderForDisk(handle)
    }
  }

  trait SpillableHostBufferHandleBuilder extends AutoCloseable {
    /**
     * Copy `mb` from offset 0 to len to host or disk.
     *
     * We synchronize after each copy since we do not manage the lifetime
     * of `mb`.
     *
     * @param mb buffer to copy from
     * @param len the amount of bytes that should be copied from `mb`
     * @param stream CUDA stream to use, and synchronize against
     */
    def copyNext(mb: DeviceMemoryBuffer, len: Long, stream: Cuda.Stream): Unit

    /**
     * Returns a usable `SpillableHostBufferHandle` with either the
     * `host` or `disk` set with the appropriate object.
     *
     * Note that if we are writing to disk, we are going to add a
     * new `DiskHandle` in the disk store's concurrent collection.
     *
     * @return host handle with data in host or disk
     */
    def build: SpillableHostBufferHandle
  }

  private class SpillableHostBufferHandleBuilderForHost(
    var handle: SpillableHostBufferHandle,
    var singleShotBuffer: HostMemoryBuffer)
      extends SpillableHostBufferHandleBuilder with Logging {
    private var copied = 0L

    override def copyNext(mb: DeviceMemoryBuffer, len: Long, stream: Cuda.Stream): Unit = {
      singleShotBuffer.copyFromMemoryBuffer(
        copied,
        mb,
        0,
        len,
        stream)
      copied += len
    }

    override def build: SpillableHostBufferHandle = {
      // add some sort of setter method to Host Handle
      require(handle != null, "Called build too many times")
      require(copied == handle.sizeInBytes,
        s"Expected ${handle.sizeInBytes} B but copied $copied B instead")
      handle.setHost(singleShotBuffer)
      singleShotBuffer = null
      val res = handle
      handle = null
      res
    }

    override def close(): Unit = {
      if (handle != null) {
        handle.close()
        handle = null
      }
      if (singleShotBuffer != null) {
        singleShotBuffer.close()
        singleShotBuffer = null
      }
    }
  }

  private class SpillableHostBufferHandleBuilderForDisk(
    var handle: SpillableHostBufferHandle)
      extends SpillableHostBufferHandleBuilder {
    private var copied = 0L
    private var diskHandleBuilder = DiskHandleStore.makeBuilder

    override def copyNext(mb: DeviceMemoryBuffer, len: Long, stream: Cuda.Stream): Unit = {
      SpillFramework.withHostSpillBounceBuffer { hostSpillBounceBuffer =>
        val outputChannel = diskHandleBuilder.getChannel
        withResource(mb.slice(0, len)) { slice =>
          val iter = new MemoryBufferToHostByteBufferIterator(
            slice,
            hostSpillBounceBuffer,
            Cuda.DEFAULT_STREAM)
          iter.foreach { byteBuff =>
            try {
              while (byteBuff.hasRemaining) {
                outputChannel.write(byteBuff)
              }
              copied += byteBuff.capacity()
            } finally {
              RapidsStorageUtils.dispose(byteBuff)
            }
          }
        }
      }
    }

    override def build: SpillableHostBufferHandle = {
      // add some sort of setter method to Host Handle
      require(handle != null, "Called build too many times")
      require(copied == handle.sizeInBytes,
        s"Expected ${handle.sizeInBytes} B but copied $copied B instead")
      handle.setDisk(diskHandleBuilder.build)
      val res = handle
      handle = null
      res
    }

    override def close(): Unit = {
      if (handle != null) {
        handle.close()
        handle = null
      }
      if (diskHandleBuilder!= null) {
        diskHandleBuilder.close()
        diskHandleBuilder = null
      }
    }
  }

  override protected def spillNvtxRange: NvtxId = NvtxRegistry.DISK_SPILL
}

class SpillableDeviceStore extends SpillableStore[DeviceSpillableHandle[_]] {
  override protected def spillNvtxRange: NvtxId = NvtxRegistry.DEVICE_SPILL

  override def postSpill(plan: SpillPlan): Unit = {
    // spillables is the list of handles that have to be closed
    // we synchronize every thread before we release what was spilled
    Cuda.deviceSynchronize()
    // this is safe to be called unconditionally if another thread spilled
    plan.getSpilled.forEach(_.releaseSpilled())
  }
}

class DiskHandleStore(conf: SparkConf)
    extends HandleStore[DiskHandle] with Logging {
  val diskBlockManager: RapidsDiskBlockManager = new RapidsDiskBlockManager(conf)

  def getFile(blockId: BlockId): File = {
    diskBlockManager.getFile(blockId)
  }

  def deleteFile(blockId: BlockId): Unit = {
    val file = getFile(blockId)
    file.delete()
    if (file.exists()) {
      logWarning(s"Unable to delete $file")
    }
  }

  override def track(handle: DiskHandle): Unit = {
    // protects the off chance that someone adds this handle twice..
    if (doTrack(handle)) {
      GpuTaskMetrics.get.incDiskBytesAllocated(handle.sizeInBytes)
    }
  }

  override def remove(handle: DiskHandle): Unit = {
    // protects the off chance that someone removes this handle twice..
    if (doRemove(handle)) {
      GpuTaskMetrics.get.decDiskBytesAllocated(handle.sizeInBytes)
    }
  }
}

object DiskHandleStore {
  /**
   * An object that knows how to write a block to disk in Spark.
   * It supports
   * @param blockId the BlockManager `BlockId` to use.
   * @param startPos the position to start writing from, useful if we can
   *                 share files
   */
  class DiskHandleBuilder(val blockId: BlockId,
                          val startPos: Long = 0L) extends AutoCloseable {
    private val file = SpillFramework.stores.diskStore.getFile(blockId)

    private val serializerManager =
      SpillFramework.stores.diskStore.diskBlockManager.getSerializerManager()

    // this is just to make sure we use DiskWriter once and we are not leaking
    // as it is, we could use `DiskWriter` to start writing at other offsets
    private var closed = false

    private var fc: FileChannel = _

    private def getFileChannel: FileChannel = {
      val options = Seq(StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      fc = FileChannel.open(file.toPath, options:_*)
      // seek to the starting pos
      fc.position(startPos)
      fc
    }

    private def wrapChannel(channel: FileChannel): OutputStream = {
      val os = Channels.newOutputStream(channel)
      serializerManager.wrapStream(blockId, os)
    }

    private var outputChannel: WritableByteChannel = _
    private var outputStream: DataOutputStream = _

    def getChannel: WritableByteChannel = {
      require(!closed, "Cannot write to closed DiskWriter")
      require(outputStream == null,
        "either channel or data output stream supported, but not both")
      if (outputChannel != null) {
        outputChannel
      } else {
        val fc = getFileChannel
        val wrappedStream = closeOnExcept(fc)(wrapChannel)
        outputChannel = closeOnExcept(wrappedStream)(Channels.newChannel)
        outputChannel
      }
    }

    def getDataOutputStream: DataOutputStream = {
      require(!closed, "Cannot write to closed DiskWriter")
      require(outputStream == null,
        "either channel or data output stream supported, but not both")
      if (outputStream != null) {
        outputStream
      } else {
        val fc = getFileChannel
        val wrappedStream = closeOnExcept(fc)(wrapChannel)
        outputStream = new DataOutputStream(wrappedStream)
        outputStream
      }
    }

    override def close(): Unit = {
      if (closed) {
        throw new IllegalStateException("already closed DiskWriter")
      }
      if (outputStream != null) {
        outputStream.close()
        outputStream = null
      }
      if (outputChannel != null) {
        outputChannel.close()
        outputChannel = null
      }
      closed = true
    }

    def size: Long = fc.position() - startPos

    def build: DiskHandle =
      DiskHandle(
        blockId,
        startPos,
        size)

    def build(taskPriority: Long): DiskHandle =
      DiskHandle(blockId, startPos, size, taskPriority)
  }

  def makeBuilder: DiskHandleBuilder = {
    val blockId = BlockId(s"temp_local_${UUID.randomUUID().toString}")
    new DiskHandleBuilder(blockId)
  }
}

trait SpillableStores extends AutoCloseable {
  var deviceStore: SpillableDeviceStore
  var hostStore: SpillableHostStore
  var diskStore: DiskHandleStore
  override def close(): Unit = {
    Seq(deviceStore, hostStore, diskStore).safeClose()
  }
}

/**
 * A spillable that is meant to be interacted with from the device.
 */
object SpillableColumnarBatchHandle {
  def apply(tbl: Table, dataTypes: Array[DataType]): SpillableColumnarBatchHandle = {
    withResource(tbl) { _ =>
      SpillableColumnarBatchHandle(GpuColumnVector.from(tbl, dataTypes))
    }
  }

  def apply(cb: ColumnarBatch): SpillableColumnarBatchHandle = {
    require(!GpuColumnVectorFromBuffer.isFromBuffer(cb),
      "A SpillableColumnarBatchHandle doesn't support cuDF packed batches")
    require(!GpuCompressedColumnVector.isBatchCompressed(cb),
      "A SpillableColumnarBatchHandle doesn't support comprssed batches")
    val sizeInBytes = GpuColumnVector.getTotalDeviceMemoryUsed(cb)
    val handle = new SpillableColumnarBatchHandle(sizeInBytes, dev = Some(cb))
    SpillFramework.stores.deviceStore.track(handle)
    handle
  }
}

class SpillableTableHandle private (
    override val approxSizeInBytes: Long,
    private[spill] override var dev: Option[Table],
    private[spill] var host: Option[SpillableHostBufferHandle] = None)
  extends DeviceSpillableHandle[Table] with Logging {

  override def spillable: Boolean = synchronized {
    if (super.spillable) {
      val table = dev.get
      val colRepetition = mutable.HashMap[ColumnVector, Int]()
      var i = 0
      while (i < table.getNumberOfColumns) {
        val col = table.getColumn(i)
        colRepetition.put(col, colRepetition.getOrElse(col, 0) + 1)
        i += 1
      }
      colRepetition.forall { case (col, cnt) =>
        cnt == col.getRefCount
      }
    } else {
      false
    }
  }

  private var meta: Option[ByteBuffer] = None

  def materialize(): Table = {
    var materialized: Table = null
    var hostHandle: SpillableHostBufferHandle = null
    synchronized {
      if (closed) {
        throw new IllegalStateException("attempting to materialize a closed handle")
      } else if (host.isDefined) {
        hostHandle = host.get
      } else if (dev.isDefined) {
        materialized = SpillableTableHandle.cloneTable(dev.get)
      } else {
        throw new IllegalStateException("open handle has no underlying table")
      }
    }
    if (materialized == null) {
      // (Similar as SpillableColumnarBatchHandle)
      com.nvidia.spark.rapids.jni.RmmSpark.spillRangeStart()
      try {
        val devBuffer = closeOnExcept(DeviceMemoryBuffer.allocate(hostHandle.sizeInBytes)) {
          dmb =>
            hostHandle.materializeToDeviceMemoryBuffer(dmb)
            dmb
        }
        materialized = withResource(devBuffer)(Table.fromPackedTable(meta.get, _))
      } finally {
        com.nvidia.spark.rapids.jni.RmmSpark.spillRangeDone()
      }
    }
    materialized
  }

  override def spill(): Long = {
    if (!spillable) {
      0L
    } else {
      val tblToSpill: Option[Table] = synchronized {
        if (!closed && host.isEmpty && dev.isDefined && !spilling) {
          spilling = true
          Some(SpillableTableHandle.cloneTable(dev.get))
        } else {
          None
        }
      }
      if (tblToSpill.isDefined) {
        executeSpill {
          // "tblToSpill" will be closed inside "withChunkedPacker".
          withChunkedPacker(tblToSpill.get) { chunkedPacker =>
            meta = Some(chunkedPacker.getPackedMeta)
            var staging: Option[SpillableHostBufferHandle] =
              Some(SpillableHostBufferHandle.createHostHandleWithPacker(chunkedPacker,
                taskPriority))
            val actualBytes: Long = staging.map(_.sizeInBytes).getOrElse(0L)
            synchronized {
              spilling = false
              if (closed) {
                staging.foreach(_.close())
                staging = None
                doClose()
              } else {
                host = staging
              }
            }
            actualBytes // return actual bytes spilled for metrics
          }
        }
        // We return the size we were created with. This is not the actual size
        // of this table when it is packed, and it is used by the calling code
        // to figure out more or less how much did we free in the device.
        approxSizeInBytes
      } else {
        0L
      }
    }
  }

  private def withChunkedPacker[T](tableToPack: Table)(body: ChunkedPacker => T): T = {
    withResource(tableToPack) { _ =>
      withResource(new ChunkedPacker(tableToPack, SpillFramework.chunkedPackBounceBufferPool)) {
        packer => body(packer)
      }
    }
  }

  override def doClose(): Unit = {
    releaseDeviceResource()
    host.foreach(_.close())
    host = None
  }
}

object SpillableTableHandle {
  def apply(table: Table): SpillableTableHandle = {
    val sizeInBytes = GpuColumnVector.getTotalDeviceMemoryUsed(table)
    val handle = new SpillableTableHandle(sizeInBytes, dev = Some(table))
    SpillFramework.stores.deviceStore.track(handle)
    handle
  }

  private def cloneTable(table: Table): Table = {
    val numCols = table.getNumberOfColumns
    val columns = new Array[ColumnVector](numCols)
    var i = 0
    while (i < numCols) {
      columns(i) = table.getColumn(i)
      i = i + 1
    }
    new Table(columns: _*) // The new table increases the columns ref
  }
}

object SpillFramework extends Logging {
  // public for tests. Some tests not in the `spill` package require setting this
  // because they need fine control over allocations.
  var storesInternal: SpillableStores = _

  def stores: SpillableStores = {
    if (storesInternal == null) {
      throw new IllegalStateException(
        "Cannot use SpillFramework without calling SpillFramework.initialize first")
    }
    storesInternal
  }

  private var hostSpillBounceBufferPool: BounceBufferPool[HostMemoryBuffer] = _

  private lazy val conf: SparkConf = {
    val env = SparkEnv.get
    if (env != null) {
      env.conf
    } else {
      // For some unit tests
      new SparkConf()
    }
  }

  def initialize(rapidsConf: RapidsConf): Unit = synchronized {
    require(storesInternal == null,
      s"cannot initialize SpillFramework multiple times.")

    val hostSpillStorageSize = if (rapidsConf.offHeapLimitEnabled) {
      // Disable the limit because it is handled by the RapidsHostMemoryStore
      if (rapidsConf.hostSpillStorageSize == -1) {
        logWarning(s"both ${RapidsConf.OFF_HEAP_LIMIT_ENABLED} and " +
          s"${RapidsConf.HOST_SPILL_STORAGE_SIZE} are set; using " +
          s"${RapidsConf.OFF_HEAP_LIMIT_SIZE} and ignoring ${RapidsConf.HOST_SPILL_STORAGE_SIZE}")
      }
      None
    } else if (rapidsConf.hostSpillStorageSize == -1) {
      // + 1 GiB by default to match backwards compatibility
      Some(rapidsConf.pinnedPoolSize + (1024L * 1024 * 1024))
    } else {
      Some(rapidsConf.hostSpillStorageSize)
    }
    hostSpillBounceBufferPool = new BounceBufferPool[HostMemoryBuffer](
      rapidsConf.spillToDiskBounceBufferSize,
      rapidsConf.spillToDiskBounceBufferCount,
      HostMemoryBuffer.allocate)

    chunkedPackBounceBufferPool = new BounceBufferPool[DeviceMemoryBuffer](
      rapidsConf.chunkedPackBounceBufferSize,
      rapidsConf.chunkedPackBounceBufferCount,
      DeviceMemoryBuffer.allocate)
    storesInternal = new SpillableStores {
      override var deviceStore: SpillableDeviceStore = new SpillableDeviceStore
      override var hostStore: SpillableHostStore = new SpillableHostStore(hostSpillStorageSize)
      override var diskStore: DiskHandleStore = new DiskHandleStore(conf)
    }
    val hostSpillStorageSizeStr = hostSpillStorageSize.map(sz => s"$sz B").getOrElse("unlimited")
    logInfo(s"Initialized SpillFramework. Host spill store max size is: $hostSpillStorageSizeStr.")
  }

  def getHostStoreSpillableSummary: String = {
    try {
      stores.hostStore.spillableSummary()
    } catch {
      case _: IllegalStateException =>
        "Could not log spill summary, spill framework not initialized"
    }
  }

  def getDeviceStoreSpillableSummary: String = {
    try {
      stores.deviceStore.spillableSummary()
    } catch {
      case _: IllegalStateException =>
        "Could not log spill summary, spill framework not initialized"
    }
  }

  def shutdown(): Unit = {
    if (hostSpillBounceBufferPool != null) {
      hostSpillBounceBufferPool.close()
      hostSpillBounceBufferPool = null
    }
    if (chunkedPackBounceBufferPool != null) {
      chunkedPackBounceBufferPool.close()
      chunkedPackBounceBufferPool = null
    }
    if (storesInternal != null) {
      storesInternal.close()
      storesInternal = null
    }
  }

  def withHostSpillBounceBuffer[T](body: HostMemoryBuffer => T): T = {
    withResource(hostSpillBounceBufferPool.nextBuffer()) { hmb =>
      body(hmb.buf)
    }
  }

  var chunkedPackBounceBufferPool: BounceBufferPool[DeviceMemoryBuffer] = _

  // if the stores have already shut down, we don't want to create them here
  // so we use `storesInternal` directly in these remove functions.

  private[spill] def removeFromDeviceStore(handle: DeviceSpillableHandle[_]): Unit = {
    synchronized {
      Option(storesInternal).map(_.deviceStore)
    }.foreach(_.remove(handle))
  }

  private[spill] def removeFromHostStore(handle: HostSpillableHandle[_]): Unit = {
    synchronized {
      Option(storesInternal).map(_.hostStore)
    }.foreach(_.remove(handle))
  }

  private[spill] def removeFromDiskStore(handle: DiskHandle): Unit = {
    synchronized {
      Option(storesInternal).map(_.diskStore)
    }.foreach(_.remove(handle))
  }
}

/**
 * A bounce buffer wrapper class that supports the concept of acquisition.
 *
 * The bounce buffer is acquired from a BounceBufferPool, so any calls to
 * BounceBufferPool.nextBuffer will block if the pool has no available buffers.
 *
 * `close` restores a bounce buffer to the pool for other callers to use.
 *
 * `release` actually closes the underlying buffer, and should be called
 * once at the end of the lifetime of the executor.
 *
 * @param buf - actual cudf DeviceMemoryBuffer that this class is protecting.
 * @param pool - the pool to which this buffer belongs
 */
private[spill] class BounceBuffer[T <: AutoCloseable](
                                                       var buf: T,
                                                       private val pool: BounceBufferPool[T])
  extends AutoCloseable {
  override def close(): Unit = {
    pool.returnBuffer(this)
  }

  def release(): Unit = {
    buf.close()
    buf = null.asInstanceOf[T]
  }
}


/**
 * A bounce buffer pool with buffers of size `bufSize`
 *
 * This pool returns instances of `BounceBuffer[T]`, that should
 * be closed in order to be reused.
 *
 * Callers should synchronize before calling close on their `DeviceMemoryBuffer`s.
 */
class BounceBufferPool[T <: AutoCloseable](private val bufSize: Long,
                                           private val bbCount: Int,
                                           private val allocator: Long => T)
  extends AutoCloseable with Logging {

  private val pool = new ArrayBlockingQueue[BounceBuffer[T]](bbCount)
  for (_ <- 1 to bbCount) {
    pool.offer(new BounceBuffer[T](allocator(bufSize), this))
  }

  def bufferSize: Long = bufSize
  def nextBuffer(): BounceBuffer[T] = synchronized {
    if (closed) {
      throw new IllegalStateException("tried to acquire a bounce buffer after the" +
        "pool has been closed!")
    }
    while (pool.size() <= 0) {
      waitForBuffer()
      if (closed) {
        throw new IllegalStateException("tried to acquire a bounce buffer after the" +
          "pool has been closed!")
      }
    }
    pool.take()
  }

  private def waitForBuffer(): Unit = {
    TaskRegistryTracker.withRmmPoolWait {
      wait()
    }
  }

  def returnBuffer(buffer: BounceBuffer[T]): Unit = synchronized {
    if (closed) {
      buffer.release()
    } else {
      pool.offer(buffer)
      // Wake up one thread to take the next bounce buffer
      notify()
    }
  }

  private var closed = false
  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      if (pool.size() < bbCount) {
        logError("tried to close BounceBufferPool when buffers are still " +
          "being used")
      }

      pool.forEach(_.release())
      pool.clear()
      // Wake up any threads that might be waiting still...
      notifyAll()
    }
  }
}

/**
 * ChunkedPacker is an Iterator-like class that uses a cudf::chunked_pack to copy a cuDF `Table`
 * to a target buffer in chunks. It implements a next method that takes a DeviceMemoryBuffer
 * as an argument to be used for the copy.
 *
 * Each chunk is sized at most `bounceBuffer.getLength`, and the caller should cudaMemcpy
 * bytes from `bounceBuffer` to a target buffer after each call to `next()`.
 *
 * @note `ChunkedPacker` must be closed by the caller as it has GPU and host resources
 *       associated with it.
 *
 * @param table cuDF Table to chunk_pack
 * @param bounceBufferPool bounce buffer pool to use during the lifetime of this packer.
 */
class ChunkedPacker(table: Table,
                    bounceBufferPool: BounceBufferPool[DeviceMemoryBuffer])
  extends Iterator[(BounceBuffer[DeviceMemoryBuffer], Long)] with Logging with AutoCloseable {

  private var closed: Boolean = false

  // When creating cudf::chunked_pack use a pool if available, otherwise default to the
  // per-device memory resource
  private val chunkedPack = {
    val pool = GpuDeviceManager.chunkedPackMemoryResource
    val cudfChunkedPack = try {
      pool.flatMap { chunkedPool =>
        Some(table.makeChunkedPack(bounceBufferPool.bufferSize, chunkedPool))
      }
    } catch {
      case _: OutOfMemoryError =>
        if (!ChunkedPacker.warnedAboutPoolFallback) {
          ChunkedPacker.warnedAboutPoolFallback = true
          logWarning(
            s"OOM while creating chunked_pack using pool sized ${pool.map(_.getMaxSize)}B. " +
              "Falling back to the per-device memory resource.")
        }
        None
    }

    // if the pool is not configured, or we got an OOM, try again with the per-device pool
    cudfChunkedPack.getOrElse {
      table.makeChunkedPack(bounceBufferPool.bufferSize)
    }
  }

  private val packedMeta = withResource(chunkedPack.buildMetadata()) { packedMeta =>
    val tmpBB = packedMeta.getMetadataDirectBuffer
    val metaCopy = ByteBuffer.allocateDirect(tmpBB.capacity())
    metaCopy.put(tmpBB)
    metaCopy.flip()
    metaCopy
  }

  def getTotalContiguousSize: Long = chunkedPack.getTotalContiguousSize

  def getPackedMeta: ByteBuffer = {
    packedMeta
  }

  override def hasNext: Boolean = {
    if (closed) {
      throw new IllegalStateException(s"ChunkedPacker is closed")
    }
    chunkedPack.hasNext
  }

  override def next(): (BounceBuffer[DeviceMemoryBuffer], Long) = {
    closeOnExcept(bounceBufferPool.nextBuffer()) { bounceBuffer =>
      if (closed) {
        throw new IllegalStateException(s"ChunkedPacker is closed")
      }
      val bytesWritten = chunkedPack.next(bounceBuffer.buf)
      // we increment the refcount because the caller has no idea where
      // this memory came from, so it should close it.
      (bounceBuffer, bytesWritten)
    }
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      chunkedPack.close()
    }
  }
}

private object ChunkedPacker {
  private var warnedAboutPoolFallback: Boolean = false
}
