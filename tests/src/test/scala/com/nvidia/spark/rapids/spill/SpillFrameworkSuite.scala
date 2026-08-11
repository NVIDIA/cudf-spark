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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DataType

class SpillFrameworkSuite extends SpillUnitTestBase with BeforeAndAfterAll {

  test("add table registers with device store") {
    val (ct, dataTypes) = buildContiguousTable()
    withResource(SpillableColumnarBatchFromBufferHandle(ct, dataTypes)) { _ =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    }
  }

  test("a non-contiguous table is spillable and it is handed over to the store") {
    val (tbl, dataTypes) = buildTable()
    withResource(SpillableColumnarBatchHandle(tbl, dataTypes)) { handle =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
    }
  }

  test("a non-contiguous table becomes non-spillable when batch is obtained") {
    val (tbl, dataTypes) = buildTable()
    withResource(SpillableColumnarBatchHandle(tbl, dataTypes)) { handle =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
      withResource(handle.materialize(dataTypes)) { _ =>
        assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
        assert(!handle.spillable)
        assertResult(0)(SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes))
      }
      assert(handle.spillable)
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assertResult(handle.approxSizeInBytes)(
        SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes))
    }
  }

  test("a non-contiguous table is non-spillable until all columns are returned") {
    val (table, dataTypes) = buildTable()
    withResource(SpillableColumnarBatchHandle(table, dataTypes)) { handle =>
      assert(handle.spillable)
      val cb = handle.materialize(dataTypes)
      assert(!handle.spillable)
      val columns = GpuColumnVector.extractBases(cb)
      withResource(columns.head) { _ =>
        columns.head.incRefCount()
        withResource(cb) { _ =>
          assert(!handle.spillable)
        }
        // still 0 after the batch is closed, because of the extra incRefCount
        // for columns.head
        assert(!handle.spillable)
      }
      // columns.head is closed, so now our RapidsTable is spillable again
      assert(handle.spillable)
    }
  }

  test("an aliased non-contiguous table is not spillable (until closing the alias) ") {
    val (table, dataTypes) = buildTable()
    withResource(SpillableColumnarBatchHandle(table, dataTypes)) { handle =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
      withResource(SpillableColumnarBatchHandle(handle.materialize(dataTypes))) { aliasHandle =>
        assertResult(2)(SpillFramework.stores.deviceStore.numHandles)
        assert(!handle.spillable)
        assert(!aliasHandle.spillable)
      } // we now have two copies in the store
      assert(handle.spillable)
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    }
  }

  test("an aliased contiguous table is not spillable (until closing the alias) ") {
    val (table, dataTypes) = buildContiguousTable()
    withResource(SpillableColumnarBatchFromBufferHandle(table, dataTypes)) { handle =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
      val materialized = handle.materialize(dataTypes)
      // note that materialized is a batch "from buffer", it is not a regular batch
      withResource(SpillableColumnarBatchFromBufferHandle(materialized)) { aliasHandle =>
        // we now have two copies in the store
        assertResult(2)(SpillFramework.stores.deviceStore.numHandles)
        assert(!handle.spillable)
        assert(!aliasHandle.spillable)
      }
      assert(handle.spillable)
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    }
  }

  test("an non-contiguous table supports duplicated columns") {
    val (table, dataTypes) = buildTableWithDuplicate()
    withResource(SpillableColumnarBatchHandle(table, dataTypes)) { handle =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
      withResource(SpillableColumnarBatchHandle(handle.materialize(dataTypes))) { aliasHandle =>
        assertResult(2)(SpillFramework.stores.deviceStore.numHandles)
        assert(!handle.spillable)
        assert(!aliasHandle.spillable)
      } // we now have two copies in the store
      assert(handle.spillable)
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    }
  }

  test("a buffer is not spillable until the owner closes columns referencing it") {
    val (ct, _) = buildContiguousTable()
    // the contract for spillable handles is that they take ownership
    // incRefCount to follow that pattern
    val buff = ct.getBuffer
    buff.incRefCount()
    withResource(SpillableDeviceBufferHandle(buff)) { handle =>
      withResource(ct) { _ =>
        assert(!handle.spillable)
      }
      assert(handle.spillable)
    }
  }

  test("a compressed batch can be added and recovered") {
    val ct = buildCompressedBatch(0, 1000)
    withResource(SpillableCompressedColumnarBatchHandle(ct)) { handle =>
      assert(handle.spillable)
      withResource(handle.materialize()) { materialized =>
        assert(!handle.spillable)
        // since we didn't spill, these buffers are exactly the same
        assert(
          ct.column(0).asInstanceOf[GpuCompressedColumnVector].getTableBuffer ==
            materialized.column(0).asInstanceOf[GpuCompressedColumnVector].getTableBuffer)
      }
      assert(handle.spillable)
    }
  }

  test("a compressed batch can be added and recovered after being spilled to host") {
    val ct = buildCompressedBatch(0, 1000)
    withResource(decompressBatch(ct)) { decompressedExpected =>
      withResource(SpillableCompressedColumnarBatchHandle(ct)) { handle =>
        assert(handle.spillable)
        SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
        assert(!handle.spillable)
        assert(handle.dev.isEmpty)
        assert(handle.host.isDefined)
        withResource(handle.materialize()) { materialized =>
          withResource(decompressBatch(materialized)) { decompressed =>
            TestUtils.compareBatches(decompressedExpected, decompressed)
          }
        }
      }
    }
  }

  test("a compressed batch can be added and recovered after being spilled to disk") {
    val ct = buildCompressedBatch(0, 1000)
    withResource(decompressBatch(ct)) { decompressedExpected =>
      withResource(SpillableCompressedColumnarBatchHandle(ct)) { handle =>
        assert(handle.spillable)
        SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
        assert(!handle.spillable)
        SpillFramework.stores.hostStore.spill(handle.approxSizeInBytes)
        assert(handle.dev.isEmpty)
        assert(handle.host.isDefined)
        assert(handle.host.get.host.isEmpty)
        assert(handle.host.get.disk.isDefined)
        withResource(handle.materialize()) { materialized =>
          withResource(decompressBatch(materialized)) { decompressed =>
            TestUtils.compareBatches(decompressedExpected, decompressed)
          }
        }
      }
    }
  }


  test("a second handle prevents buffer to be spilled") {
    val buffer = DeviceMemoryBuffer.allocate(123)
    val handle1 = SpillableDeviceBufferHandle(buffer)
    // materialize will incRefCount `buffer`. This looks a little weird
    // but it simulates aliasing as it happens in real code
    val handle2 = SpillableDeviceBufferHandle(handle1.materialize())

    withResource(handle1) { _ =>
      withResource(handle2) { _ =>
        assertResult(2)(handle1.dev.get.getRefCount)
        assertResult(2)(handle2.dev.get.getRefCount)
        assertResult(false)(handle1.spillable)
        assertResult(false)(handle2.spillable)
      }
      assertResult(1)(handle1.dev.get.getRefCount)
      assertResult(true)(handle1.spillable)
    }
  }

  test("removing handle releases buffer resources in all stores") {
    val handle = SpillableDeviceBufferHandle(DeviceMemoryBuffer.allocate(123))
    withResource(handle) { _ =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assertResult(0)(SpillFramework.stores.hostStore.numHandles)
      assertResult(0)(SpillFramework.stores.diskStore.numHandles)

      assertResult(123)(SpillFramework.stores.deviceStore.spill(123)) // spill to host memory
      assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
      assertResult(1)(SpillFramework.stores.hostStore.numHandles)
      assertResult(0)(SpillFramework.stores.diskStore.numHandles)
      assert(handle.dev.isEmpty)
      assert(handle.host.isDefined)
      assert(handle.host.get.host.isDefined)

      assertResult(123)(SpillFramework.stores.hostStore.spill(123)) // spill to disk
      assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
      assertResult(0)(SpillFramework.stores.hostStore.numHandles)
      assertResult(1)(SpillFramework.stores.diskStore.numHandles)
      assert(handle.dev.isEmpty)
      assert(handle.host.isDefined)
      assert(handle.host.get.host.isEmpty)
      assert(handle.host.get.disk.isDefined)
    }
    assert(handle.host.isEmpty)
    assert(handle.dev.isEmpty)
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
    assertResult(0)(SpillFramework.stores.hostStore.numHandles)
    assertResult(0)(SpillFramework.stores.diskStore.numHandles)
  }

  test("spill updates store state") {
    val diskStore = SpillFramework.stores.diskStore
    val hostStore = SpillFramework.stores.hostStore
    val deviceStore = SpillFramework.stores.deviceStore

    val (bufferSize, handle, _) =
      addContiguousTableToFramework()

    withResource(handle) { _ =>
      assertResult(1)(deviceStore.numHandles)
      assertResult(0)(diskStore.numHandles)
      assertResult(0)(hostStore.numHandles)

      assertResult(bufferSize)(SpillFramework.stores.deviceStore.spill(bufferSize))
      assertResult(bufferSize)(SpillFramework.stores.hostStore.spill(bufferSize))

      assertResult(0)(deviceStore.numHandles)
      assertResult(0)(hostStore.numHandles)
      assertResult(1)(diskStore.numHandles)

      val diskHandle = handle.host.flatMap(_.disk).get
      val path = diskStore.getFile(diskHandle.blockId)
      assert(path.exists)
    }
  }

  test("get columnar batch after host spill") {
    val (ct, dataTypes) = buildContiguousTable()
    val expectedBatch = GpuColumnVector.from(ct.getTable, dataTypes)
    withResource(SpillableColumnarBatchFromBufferHandle(
      ct, dataTypes)) { handle =>
      withResource(expectedBatch) { _ =>
        SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
        withResource(handle.materialize(dataTypes)) { cb =>
          TestUtils.compareBatches(expectedBatch, cb)
        }
      }
    }
  }

  test("get memory buffer after host spill") {
    val (ct, dataTypes) = buildContiguousTable()
    val expectedBatch = closeOnExcept(ct) { _ =>
      // make a copy of the table so we can compare it later to the
      // one reconstituted after the spill
      withResource(ct.getTable.contiguousSplit()) { copied =>
        GpuColumnVector.from(copied(0).getTable, dataTypes)
      }
    }
    val handle = SpillableColumnarBatchFromBufferHandle(ct, dataTypes)
    withResource(handle) { _ =>
      withResource(expectedBatch) { _ =>
        assertResult(SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes))(
          handle.approxSizeInBytes)
        val hostSize = handle.host.get.approxSizeInBytes
        assertResult(SpillFramework.stores.hostStore.spill(hostSize))(hostSize)
        withResource(handle.materialize(dataTypes)) { actualBatch =>
          TestUtils.compareBatches(expectedBatch, actualBatch)
        }
      }
    }
  }

  test("host originated: get host memory buffer") {
    val hmb = HostMemoryBuffer.allocate(1L * 1024)
    val spillableBuffer = SpillableHostBuffer(hmb, hmb.getLength,
      SpillPriorities.ACTIVE_BATCHING_PRIORITY)
    withResource(spillableBuffer) { _ =>
      // the refcount of 1 is the store
      assertResult(1)(hmb.getRefCount)
      withResource(spillableBuffer.getHostBuffer()) { memoryBuffer =>
        assertResult(hmb)(memoryBuffer)
        assertResult(2)(memoryBuffer.getRefCount)
      }
    }
    assertResult(0)(hmb.getRefCount)
  }

  test("host originated: get host memory buffer after spill to disk") {
    val hmb = HostMemoryBuffer.allocate(1L * 1024)
    val spillableBuffer = SpillableHostBuffer(
      hmb,
      hmb.getLength,
      SpillPriorities.ACTIVE_BATCHING_PRIORITY)
    assertResult(1)(hmb.getRefCount)
    //  we spill it
    SpillFramework.stores.hostStore.spill(hmb.getLength)
    withResource(spillableBuffer) { _ =>
      // the refcount of the original buffer is 0 because it spilled
      assertResult(0)(hmb.getRefCount)
      withResource(spillableBuffer.getHostBuffer()) { memoryBuffer =>
        assertResult(memoryBuffer.getLength)(hmb.getLength)
      }
    }
  }

  test("host originated: a buffer is not spillable when we leak it") {
    val hmb = HostMemoryBuffer.allocate(1L * 1024)
    withResource(SpillableHostBuffer(hmb, hmb.getLength,
        SpillPriorities.ACTIVE_BATCHING_PRIORITY)) { spillableBuffer =>
      withResource(spillableBuffer.getHostBuffer()) { _ =>
        assertResult(0)(SpillFramework.stores.hostStore.spill(hmb.getLength))
      }
      assertResult(hmb.getLength)(SpillFramework.stores.hostStore.spill(hmb.getLength))
    }
  }

  test("host originated: a host batch is not spillable when we leak it") {
    val (hostCb, sparkTypes) = buildHostBatch()
    val sizeOnHost = RapidsHostColumnVector.getTotalHostMemoryUsed(hostCb)
    withResource(SpillableHostColumnarBatchHandle(hostCb)) { handle =>
      assertResult(true)(handle.spillable)

      withResource(handle.materialize(sparkTypes)) { _ =>
        // 0 because we have a reference to the host batch
        assertResult(false)(handle.spillable)
        assertResult(0)(SpillFramework.stores.hostStore.spill(sizeOnHost))
      }

      // after closing we still have 0 bytes in the store or available to spill
      assertResult(true)(handle.spillable)
    }
  }

  test("host originated: a host batch is not spillable when columns are incRefCounted") {
    val (hostCb, sparkTypes) = buildHostBatch()
    val sizeOnHost = RapidsHostColumnVector.getTotalHostMemoryUsed(hostCb)
    withResource(SpillableHostColumnarBatchHandle(hostCb)) { handle =>
      assertResult(true)(handle.spillable)
      val leakedFirstColumn = withResource(handle.materialize(sparkTypes)) { cb =>
        // 0 because we have a reference to the host batch
        assertResult(false)(handle.spillable)
        assertResult(0)(SpillFramework.stores.hostStore.spill(sizeOnHost))
        // leak it by increasing the ref count of the underlying cuDF column
        RapidsHostColumnVector.extractBases(cb).head.incRefCount()
      }
      withResource(leakedFirstColumn) { _ =>
        // 0 because we have a reference to the first column
        assertResult(false)(handle.spillable)
        assertResult(0)(SpillFramework.stores.hostStore.spill(sizeOnHost))
      }
      // batch is now spillable because we close our reference to the column
      assertResult(true)(handle.spillable)
      assertResult(sizeOnHost)(SpillFramework.stores.hostStore.spill(sizeOnHost))
    }
  }

  test("host originated: an aliased host batch is not spillable (until closing the original) ") {
    val (hostBatch, sparkTypes) = buildHostBatch()
    val handle = SpillableHostColumnarBatchHandle(hostBatch)
    withResource(handle) { _ =>
      assertResult(1)(SpillFramework.stores.hostStore.numHandles)
      assertResult(true)(handle.spillable)
      withResource(handle.materialize(sparkTypes)) { _ =>
        assertResult(false)(handle.spillable)
      } // we now have two copies in the store
      assertResult(true)(handle.spillable)
    }
  }

  test("host originated: a host batch supports duplicated columns") {
    val (hostBatch, sparkTypes) = buildHostBatchWithDuplicate()
    val handle = SpillableHostColumnarBatchHandle(hostBatch)
    withResource(handle) { _ =>
      assertResult(1)(SpillFramework.stores.hostStore.numHandles)
      assertResult(true)(handle.spillable)
      withResource(handle.materialize(sparkTypes)) { _ =>
        assertResult(false)(handle.spillable)
      } // we now have two copies in the store
      assertResult(true)(handle.spillable)
    }
  }

  test("host originated: a host batch supports aliasing and duplicated columns") {
    SpillFramework.shutdown()
    val sc = new SparkConf
    // disables the host store limit by enabling off heap limits
    sc.set(RapidsConf.OFF_HEAP_LIMIT_ENABLED.key, "true")
    SpillFramework.initialize(new RapidsConf(sc))

    try {
      val (hostBatch, sparkTypes) = buildHostBatchWithDuplicate()
      withResource(SpillableHostColumnarBatchHandle(hostBatch)) { handle =>
        withResource(SpillableHostColumnarBatchHandle(handle.materialize(sparkTypes))) { handle2 =>
          assertResult(2)(SpillFramework.stores.hostStore.numHandles)
          assertResult(false)(handle.spillable)
          assertResult(false)(handle2.spillable)
        }
        assertResult(true)(handle.spillable)
      }
    } finally {
      SpillFramework.shutdown()
    }
  }

  // this is a key behavior that we wanted to keep during the spill refactor
  // where host objects that are added directly to the store do not cause a
  // host->disk spill on their own, instead they will get spilled later
  // due to device->host spills.
  test("host factory methods do not spill on addition") {
    SpillFramework.shutdown()
    val sc = new SparkConf
    // set a very small store size
    sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, "1KB")
    sc.set(RapidsConf.OFF_HEAP_LIMIT_ENABLED.key, "false")
    SpillFramework.initialize(new RapidsConf(sc))

    try {
      // add a lot of batches, surpassing the limits of the store
      val handles = new ArrayBuffer[SpillableHostColumnarBatchHandle]()
      var dataTypes: Array[DataType] = null
      (0 until 100).foreach { _ =>
        val (hostBatch, dt) = buildHostBatch()
        if (dataTypes == null) {
          dataTypes = dt
        }
        handles.append(SpillableHostColumnarBatchHandle(hostBatch))
      }
      // no spill to disk
      assertResult(100)(SpillFramework.stores.hostStore.numHandles)

      val dmb = DeviceMemoryBuffer.allocate(1024)
      withResource(SpillableDeviceBufferHandle(dmb)) { _ =>
        // simulate an OOM by spilling device memory
        SpillFramework.stores.deviceStore.spill(1024)
        assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
      }

      val buffersSpilledToDisk = SpillFramework.stores.diskStore.numHandles
      // we spilled to disk
      assert(SpillFramework.stores.diskStore.numHandles > 0)
      // and the remaining objects that didn't spill, are still in the host store
      assertResult(100 - buffersSpilledToDisk)(SpillFramework.stores.hostStore.numHandles)
      assert(SpillFramework.stores.hostStore.totalSize <= 1024)
    } finally {
      SpillFramework.shutdown()
    }
  }

  test("direct spill to disk: when buffer exceeds maximum size") {
    var (bigTable, sparkTypes) = buildTableOfLongs(2 * 1024 * 1024)
    closeOnExcept(bigTable) { _ =>
      // make a copy of the table so we can compare it later to the
      // one reconstituted after the spill
      val expectedBatch =
        withResource(bigTable.getTable.contiguousSplit()) { expectedTable =>
          GpuColumnVector.from(expectedTable(0).getTable, sparkTypes)
        }
      withResource(expectedBatch) { _ =>
        withResource(SpillableColumnarBatchFromBufferHandle(
          bigTable, sparkTypes)) { bigHandle =>
          bigTable = null
          withResource(bigHandle.materialize(sparkTypes)) { actualBatch =>
            TestUtils.compareBatches(expectedBatch, actualBatch)
          }
          SpillFramework.stores.deviceStore.spill(bigHandle.approxSizeInBytes)
          assertResult(true)(bigHandle.dev.isEmpty)
          assertResult(true)(bigHandle.host.get.host.isEmpty)
          assertResult(false)(bigHandle.host.get.disk.isEmpty)

          withResource(bigHandle.materialize(sparkTypes)) { actualBatch =>
            TestUtils.compareBatches(expectedBatch, actualBatch)
          }
        }
      }
    }
  }

  test("get columnar batch after spilling to disk") {
    val (size, handle, dataTypes) = addContiguousTableToFramework()
    val diskStore = SpillFramework.stores.diskStore
    val hostStore = SpillFramework.stores.hostStore
    val deviceStore = SpillFramework.stores.deviceStore
    withResource(handle) { _ =>
      assertResult(1)(deviceStore.numHandles)
      assertResult(0)(diskStore.numHandles)
      assertResult(0)(hostStore.numHandles)

      val expectedTable =
        withResource(handle.materialize(dataTypes)) { beforeSpill =>
          withResource(GpuColumnVector.from(beforeSpill)) { table =>
            table.contiguousSplit()(0)
          }
        } // closing the batch from the store so that we can spill it

      withResource(expectedTable) { _ =>
        withResource(
          GpuColumnVector.from(expectedTable.getTable, dataTypes)) { expectedBatch =>
          deviceStore.spill(size)
          hostStore.spill(size)

          assertResult(0)(deviceStore.numHandles)
          assertResult(0)(hostStore.numHandles)
          assertResult(1)(diskStore.numHandles)

          val diskHandle = handle.host.flatMap(_.disk).get
          val path = diskStore.getFile(diskHandle.blockId)
          assert(path.exists)
          withResource(handle.materialize(dataTypes)) { actualBatch =>
            TestUtils.compareBatches(expectedBatch, actualBatch)
          }
        }
      }
    }
  }

  hostSpillStorageSizes.foreach { hostSpillStorageSize =>
    spillToDiskBounceBuffers.foreach { spillToDiskBounceBufferSize =>
      chunkedPackBounceBuffers.foreach { chunkedPackBounceBufferSize =>
        test("materialize non-contiguous batch after " +
          s"host_storage_size=$hostSpillStorageSize " +
          s"spilling chunked_pack_bb=$chunkedPackBounceBufferSize " +
          s"spill_to_disk_bb=$spillToDiskBounceBufferSize") {
          SpillFramework.shutdown()
          try {
            val sc = new SparkConf
            sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, hostSpillStorageSize)
            sc.set(RapidsConf.CHUNKED_PACK_BOUNCE_BUFFER_SIZE.key, chunkedPackBounceBufferSize)
            sc.set(RapidsConf.SPILL_TO_DISK_BOUNCE_BUFFER_SIZE.key, spillToDiskBounceBufferSize)
            sc.set(RapidsConf.OFF_HEAP_LIMIT_ENABLED.key, "false")
            SpillFramework.initialize(new RapidsConf(sc))
            val (largeTable, dataTypes) = buildNonContiguousTableOfLongs(numRows = 1000000)
            val handle = SpillableColumnarBatchHandle(largeTable, dataTypes)
            val diskStore = SpillFramework.stores.diskStore
            val hostStore = SpillFramework.stores.hostStore
            val deviceStore = SpillFramework.stores.deviceStore
            withResource(handle) { _ =>
              assertResult(1)(deviceStore.numHandles)
              assertResult(0)(diskStore.numHandles)
              assertResult(0)(hostStore.numHandles)

              val expectedTable =
                withResource(handle.materialize(dataTypes)) { beforeSpill =>
                  withResource(GpuColumnVector.from(beforeSpill)) { table =>
                    table.contiguousSplit()(0)
                  }
                } // closing the batch from the store so that we can spill it

              withResource(expectedTable) { _ =>
                withResource(
                  GpuColumnVector.from(expectedTable.getTable, dataTypes)) { expectedBatch =>
                  deviceStore.spill(handle.approxSizeInBytes)
                  hostStore.spill(handle.approxSizeInBytes)

                  assertResult(0)(deviceStore.numHandles)
                  assertResult(0)(hostStore.numHandles)
                  assertResult(1)(diskStore.numHandles)

                  val diskHandle = handle.host.flatMap(_.disk).get
                  val path = diskStore.getFile(diskHandle.blockId)
                  assert(path.exists)
                  withResource(handle.materialize(dataTypes)) { actualBatch =>
                    TestUtils.compareBatches(expectedBatch, actualBatch)
                  }
                }
              }
            }
          } finally {
            SpillFramework.shutdown()
          }
        }
      }
    }
  }

  test("get memory buffer after spilling to disk") {
    val handle = SpillableDeviceBufferHandle(DeviceMemoryBuffer.allocate(123))
    val diskStore = SpillFramework.stores.diskStore
    val hostStore = SpillFramework.stores.hostStore
    val deviceStore = SpillFramework.stores.deviceStore
    withResource(handle) { _ =>
      assertResult(1)(deviceStore.numHandles)
      assertResult(0)(diskStore.numHandles)
      assertResult(0)(hostStore.numHandles)
      val expectedBuffer =
        withResource(handle.materialize()) { devbuf =>
          closeOnExcept(HostMemoryBuffer.allocate(devbuf.getLength)) { hostbuf =>
            hostbuf.copyFromDeviceBuffer(devbuf)
            hostbuf
          }
        }
      withResource(expectedBuffer) { expectedBuffer =>
        deviceStore.spill(handle.approxSizeInBytes)
        hostStore.spill(handle.approxSizeInBytes)
        withResource(handle.host.map(_.materialize()).get) { actualHostBuffer =>
          assertResult(expectedBuffer.
            asByteBuffer.limit())(actualHostBuffer.asByteBuffer.limit())
        }
      }
    }
  }

  test("Compression on with or without encryption for spill block using single batch") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.io.compression.codec", "zstd")
      conf.set("spark.shuffle.spill.compress", "true")
      conf.set("spark.shuffle.compress", "true")
      readWriteTestWithBatches(conf, false)
    }
  }

  test("Compression off with or without encryption for spill block using single batch") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.shuffle.spill.compress", "false")
      conf.set("spark.shuffle.compress", "false")
      readWriteTestWithBatches(conf, false)
    }
  }

  test("Compression on with or without encryption for spill block using multiple batches") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.io.compression.codec", "zstd")
      conf.set("spark.shuffle.spill.compress", "true")
      conf.set("spark.shuffle.compress", "true")
      readWriteTestWithBatches(conf, false)
    }
  }

  test("Compression off with or without encryption for spill block using multiple batches") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.shuffle.spill.compress", "false")
      conf.set("spark.shuffle.compress", "false")
      readWriteTestWithBatches(conf, false)
    }
  }

  // ===== Tests for shuffle block =====

  test("Compression on with or without encryption for shuffle block using single batch") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.io.compression.codec", "zstd")
      conf.set("spark.shuffle.spill.compress", "true")
      conf.set("spark.shuffle.compress", "true")
      readWriteTestWithBatches(conf, true)
    }
  }

  test("Compression off with or without encryption for shuffle block using single batch") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.shuffle.spill.compress", "false")
      conf.set("spark.shuffle.compress", "false")
      readWriteTestWithBatches(conf, true)
    }
  }

  test("Compression on with or without encryption for shuffle block using multiple batches") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.io.compression.codec", "zstd")
      conf.set("spark.shuffle.spill.compress", "true")
      conf.set("spark.shuffle.compress", "true")
      readWriteTestWithBatches(conf, true, true)
    }
  }

  test("Compression off with or without encryption for shuffle block using multiple batches") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.shuffle.spill.compress", "false")
      conf.set("spark.shuffle.compress", "false")
      readWriteTestWithBatches(conf, true, true)
    }
  }

  test("No encryption and compression for shuffle block using multiple batches") {
    readWriteTestWithBatches(new SparkConf(), true, true)
  }

  test("skip host: spill device memory buffer to disk") {
    SpillFramework.shutdown()
    try {
      val sc = new SparkConf
      // disables the host store limit
      sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, "1KB")
      sc.set(RapidsConf.OFF_HEAP_LIMIT_ENABLED.key, "false")
      SpillFramework.initialize(new RapidsConf(sc))
      // buffer is too big for host store limit, so we will skip host
      val handle = SpillableDeviceBufferHandle(DeviceMemoryBuffer.allocate(1025))
      val deviceStore = SpillFramework.stores.deviceStore
      withResource(handle) { _ =>
        val expectedBuffer =
          withResource(handle.materialize()) { devbuf =>
            closeOnExcept(HostMemoryBuffer.allocate(devbuf.getLength)) { hostbuf =>
              hostbuf.copyFromDeviceBuffer(devbuf)
              hostbuf
            }
          }

        withResource(expectedBuffer) { _ =>
          // host store will fail to spill
          deviceStore.spill(handle.approxSizeInBytes)
          assert(handle.host.map(_.host.isEmpty).get)
          assert(handle.host.map(_.disk.isDefined).get)
          withResource(handle.host.map(_.materialize()).get) { buffer =>
            assertResult(expectedBuffer.asByteBuffer)(buffer.asByteBuffer)
          }
        }
      }
    } finally {
      SpillFramework.shutdown()
    }
  }

  test("skip host: spill table to disk") {
    SpillFramework.shutdown()
    try {
      val sc = new SparkConf
      sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, "1KB")
      sc.set(RapidsConf.OFF_HEAP_LIMIT_ENABLED.key, "false")
      SpillFramework.initialize(new RapidsConf(sc))
      // fill up the host store
      withResource(SpillableHostBufferHandle(HostMemoryBuffer.allocate(1024))) { hostHandle =>
        // make sure the host handle isn't spillable
        withResource(hostHandle.materialize()) { _ =>
          val (handle, _) = addTableToFramework()
          withResource(handle) { _ =>
            val (expectedTable, dataTypes) = buildTable()
            withResource(expectedTable) { _ =>
              withResource(
                GpuColumnVector.from(expectedTable, dataTypes)) { expectedBatch =>
                SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
                assert(handle.host.map(_.host.isEmpty).get)
                assert(handle.host.map(_.disk.isDefined).get)
                withResource(handle.materialize(dataTypes)) { fromDiskBatch =>
                  TestUtils.compareBatches(expectedBatch, fromDiskBatch)
                  assert(handle.dev.isEmpty)
                  assert(handle.host.map(_.host.isEmpty).get)
                  assert(handle.host.map(_.disk.isDefined).get)
                }
              }
            }
          }
        }
      }
    } finally {
      SpillFramework.shutdown()
    }
  }

  test("skip host: spill table to disk with small host bounce buffer") {
    try {
      SpillFramework.shutdown()
      val sc = new SparkConf
      // make this super small so we skip the host
      sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, "1")
      sc.set(RapidsConf.SPILL_TO_DISK_BOUNCE_BUFFER_SIZE.key, "10")
      sc.set(RapidsConf.CHUNKED_PACK_BOUNCE_BUFFER_SIZE.key, "1MB")
      sc.set(RapidsConf.OFF_HEAP_LIMIT_ENABLED.key, "false")
      val rapidsConf = new RapidsConf(sc)
      SpillFramework.initialize(rapidsConf)
      val (handle, _) = addTableToFramework()
      withResource(handle) { _ =>
        val (expectedTable, dataTypes) = buildTable()
        withResource(expectedTable) { _ =>
          withResource(
            GpuColumnVector.from(expectedTable, dataTypes)) { expectedBatch =>
            SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
            assert(handle.dev.isEmpty)
            assert(handle.host.map(_.host.isEmpty).get)
            assert(handle.host.map(_.disk.isDefined).get)
            withResource(handle.materialize(dataTypes)) { fromDiskBatch =>
              TestUtils.compareBatches(expectedBatch, fromDiskBatch)
            }
          }
        }
      }
    } finally {
      SpillFramework.shutdown()
    }
  }

  test("0-byte table is never spillable") {
    val (handle, _) = addZeroRowsTableToFramework()
    val (handle2, _) = addTableToFramework()

    withResource(handle) { _ =>
      withResource(handle2) { _ =>
        assert(handle2.host.isEmpty)
        val (expectedTable, expectedTypes) = buildTable()
        withResource(expectedTable) { _ =>
          withResource(
            GpuColumnVector.from(expectedTable, expectedTypes)) { expectedCb =>
            SpillFramework.stores.deviceStore.spill(
              handle.approxSizeInBytes + handle2.approxSizeInBytes)
            SpillFramework.stores.hostStore.spill(
              handle.approxSizeInBytes + handle2.approxSizeInBytes)
            // the 0-byte table never moved from device. It is not spillable
            assert(handle.host.isEmpty)
            assert(!handle.spillable)
            // the second table (with rows) did spill
            assert(handle2.host.isDefined)
            assert(handle2.host.map(_.host.isEmpty).get)
            assert(handle2.host.map(_.disk.isDefined).get)

            withResource(handle2.materialize(expectedTypes)) { spilledBatch =>
              TestUtils.compareBatches(expectedCb, spilledBatch)
            }
          }
        }
      }
    }
  }

  test("exclusive spill files are deleted when buffer deleted") {
    testBufferFileDeletion(canShareDiskPaths = false)
  }

  test("shared spill files are not deleted when a buffer is deleted") {
    testBufferFileDeletion(canShareDiskPaths = true)
  }

  test("a non-contiguous table close while spilling") {
    monteCarlo { sleepBeforeCloseNanos =>
      val (tbl, dataTypes) = buildTable()
      val handle = SpillableColumnarBatchHandle(tbl, dataTypes)
      testCloseWhileSpilling(handle, SpillFramework.stores.deviceStore, sleepBeforeCloseNanos)
    }
  }

  test("a device buffer close while spilling") {
    monteCarlo { sleepBeforeCloseNanos =>
      val (ct, _) = buildContiguousTable()
      // the contract for spillable handles is that they take ownership
      // incRefCount to follow that pattern
      val buff = ct.getBuffer
      buff.incRefCount()
      val handle = SpillableDeviceBufferHandle(buff)
      ct.close()
      testCloseWhileSpilling(handle, SpillFramework.stores.deviceStore, sleepBeforeCloseNanos)
    }
  }

  test("host columnar batch close while spilling") {
    monteCarlo { sleepBeforeCloseNanos =>
      val (hostCb, _) = buildHostBatch()
      val handle = SpillableHostColumnarBatchHandle(hostCb)
      testCloseWhileSpilling(handle, SpillFramework.stores.hostStore, sleepBeforeCloseNanos)
    }
  }

  test("host memory buffer close while spilling") {
    monteCarlo { sleepBeforeCloseNanos =>
      val handle = SpillableHostBufferHandle(HostMemoryBuffer.allocate(1024))
      testCloseWhileSpilling(handle, SpillFramework.stores.hostStore, sleepBeforeCloseNanos)
    }
  }

  test("cb from buffer handle close while spilling") {
    monteCarlo { sleepBeforeCloseNanos =>
      val (ct, dataTypes) = buildContiguousTable()
      val handle = SpillableColumnarBatchFromBufferHandle(ct, dataTypes)
      testCloseWhileSpilling(handle, SpillFramework.stores.deviceStore, sleepBeforeCloseNanos)
    }
  }

  test("compressed cb handle close while spilling") {
    monteCarlo { sleepBeforeCloseNanos =>
      val ct = buildCompressedBatch(0, 1000)
      val handle = SpillableCompressedColumnarBatchHandle(ct)
      testCloseWhileSpilling(handle, SpillFramework.stores.deviceStore, sleepBeforeCloseNanos)
    }
  }

}
