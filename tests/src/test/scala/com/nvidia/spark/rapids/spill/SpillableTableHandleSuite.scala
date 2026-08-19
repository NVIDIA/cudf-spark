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

package com.nvidia.spark.rapids.spill

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.SparkConf

/**
 * Unit tests for SpillableTableHandle with SpillFramework
 */
class SpillableTableHandleSuite extends SpillUnitTestBase {

  test("add table registers with device store") {
    val (tbl, _) = buildTable()
    withResource(SpillableTableHandle(tbl)) { _ =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    }
  }

  test("table is spillable and handed over to the store") {
    val (handle, _) = addSpillableTableToFramework()
    withResource(handle) { _ =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
    }
  }

  test("table becomes non-spillable when materialized") {
    val (handle, _) = addSpillableTableToFramework()
    withResource(handle) { _ =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
      withResource(handle.materialize()) { _ =>
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

  test("table is non-spillable until all columns are returned") {
    val (handle, _) = addSpillableTableToFramework()
    withResource(handle) { _ =>
      assert(handle.spillable)
      val tbl = handle.materialize()
      assert(!handle.spillable)
      val firstColumn = tbl.getColumn(0)
      withResource(firstColumn) { _ =>
        firstColumn.incRefCount()
        withResource(tbl) { _ =>
          assert(!handle.spillable)
        }
        // still not spillable after the table is closed, because of the extra incRefCount
        assert(!handle.spillable)
      }
      // firstColumn is closed, so now SpillableTableHandle is spillable again
      assert(handle.spillable)
    }
  }

  test("aliased table is not spillable (until closing the alias)") {
    val (handle, _) = addSpillableTableToFramework()
    withResource(handle) { _ =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
      withResource(SpillableTableHandle(handle.materialize())) { aliasHandle =>
        assertResult(2)(SpillFramework.stores.deviceStore.numHandles)
        assert(!handle.spillable)
        assert(!aliasHandle.spillable)
      }
      assert(handle.spillable)
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    }
  }

  test("supports duplicated columns") {
    val (handle, _) = addSpillableTableWithDuplicateToFramework()
    withResource(handle) { _ =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assert(handle.spillable)
      withResource(SpillableTableHandle(handle.materialize())) { aliasHandle =>
        assertResult(2)(SpillFramework.stores.deviceStore.numHandles)
        assert(!handle.spillable)
        assert(!aliasHandle.spillable)
      }
      assert(handle.spillable)
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    }
  }

  test("get table after host spill") {
    val (expectedTbl, _) = buildTable()
    withResource(expectedTbl) { _ =>
      val (handle, _) = addSpillableTableToFramework()
      withResource(handle) { _ =>
        SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
        assert(handle.dev.isEmpty)
        assert(handle.host.isDefined)
        withResource(handle.materialize()) { actualTbl =>
          TestUtils.compareTables(expectedTbl, actualTbl)
        }
      }
    }
  }

  test("get table after disk spill") {
    val (expectedTbl, _) = buildTable()
    withResource(expectedTbl) { _ =>
      val (handle, _) = addSpillableTableToFramework()
      withResource(handle) { _ =>
        SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
        SpillFramework.stores.hostStore.spill(handle.approxSizeInBytes)
        assert(handle.dev.isEmpty)
        assert(handle.host.isDefined)
        assert(handle.host.get.host.isEmpty)
        assert(handle.host.get.disk.isDefined)
        withResource(handle.materialize()) { actualTbl =>
          TestUtils.compareTables(expectedTbl, actualTbl)
        }
      }
    }
  }

  test("spill updates store state") {
    val diskStore = SpillFramework.stores.diskStore
    val hostStore = SpillFramework.stores.hostStore
    val deviceStore = SpillFramework.stores.deviceStore

    val (handle, _) = addSpillableTableToFramework()
    withResource(handle) { _ =>
      assertResult(1)(deviceStore.numHandles)
      assertResult(0)(diskStore.numHandles)
      assertResult(0)(hostStore.numHandles)

      assertResult(handle.approxSizeInBytes)(
        SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes))
      assertResult(handle.host.get.approxSizeInBytes)(
        SpillFramework.stores.hostStore.spill(handle.host.get.approxSizeInBytes))

      assertResult(0)(deviceStore.numHandles)
      assertResult(0)(hostStore.numHandles)
      assertResult(1)(diskStore.numHandles)

      val diskHandle = handle.host.flatMap(_.disk).get
      val path = diskStore.getFile(diskHandle.blockId)
      assert(path.exists)
    }
  }

  test("removing handle releases resources in all stores") {
    val (handle, _) = addSpillableTableToFramework()
    withResource(handle) { _ =>
      assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
      assertResult(0)(SpillFramework.stores.hostStore.numHandles)
      assertResult(0)(SpillFramework.stores.diskStore.numHandles)

      SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
      assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
      assertResult(1)(SpillFramework.stores.hostStore.numHandles)
      assertResult(0)(SpillFramework.stores.diskStore.numHandles)
      assert(handle.dev.isEmpty)
      assert(handle.host.isDefined)
      assert(handle.host.get.host.isDefined)

      SpillFramework.stores.hostStore.spill(handle.host.get.approxSizeInBytes)
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

  test("skip host - spill table to disk") {
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
          val (handle, _) = addSpillableTableToFramework()
          withResource(handle) { _ =>
            val (expectedTable, _) = buildTable()
            withResource(expectedTable) { _ =>
              SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes)
              assert(handle.host.map(_.host.isEmpty).get)
              assert(handle.host.map(_.disk.isDefined).get)
              withResource(handle.materialize()) { fromDiskTable =>
                TestUtils.compareTables(expectedTable, fromDiskTable)
                assert(handle.dev.isEmpty)
                assert(handle.host.map(_.host.isEmpty).get)
                assert(handle.host.map(_.disk.isDefined).get)
              }
            }
          }
        }
      }
    } finally {
      SpillFramework.shutdown()
    }
  }

  test("close while spilling") {
    monteCarlo { sleepBeforeCloseNanos =>
      val (tbl, _) = buildTable()
      val handle = SpillableTableHandle(tbl)
      testCloseWhileSpilling(handle, SpillFramework.stores.deviceStore, sleepBeforeCloseNanos)
    }
  }

  test("0-byte table is never spillable") {
    val (emptyTbl, _) = buildEmptyTable()
    val emptyHandle = SpillableTableHandle(emptyTbl)
    val (handle, _) = addSpillableTableToFramework()

    withResource(emptyHandle) { _ =>
      withResource(handle) { _ =>
        assert(handle.host.isEmpty)
        val (expectedTable, _) = buildTable()
        withResource(expectedTable) { _ =>
          SpillFramework.stores.deviceStore.spill(
            emptyHandle.approxSizeInBytes + handle.approxSizeInBytes)
          SpillFramework.stores.hostStore.spill(
            emptyHandle.approxSizeInBytes + handle.approxSizeInBytes)
          // the 0-byte table never moved from device. It is not spillable
          assert(emptyHandle.host.isEmpty)
          assert(!emptyHandle.spillable)
          // the second table (with rows) did spill
          assert(handle.host.isDefined)
          assert(handle.host.map(_.host.isEmpty).get)
          assert(handle.host.map(_.disk.isDefined).get)

          withResource(handle.materialize()) { spilledTable =>
            TestUtils.compareTables(expectedTable, spilledTable)
          }
        }
      }
    }
  }

  // Test with different host storage sizes and bounce buffer configurations
  hostSpillStorageSizes.foreach { hostSpillStorageSize =>
    spillToDiskBounceBuffers.foreach { spillToDiskBounceBufferSize =>
      chunkedPackBounceBuffers.foreach { chunkedPackBounceBufferSize =>
        test(s"materialize table after spilling " +
          s"host_storage_size=$hostSpillStorageSize " +
          s"chunked_pack_bb=$chunkedPackBounceBufferSize " +
          s"spill_to_disk_bb=$spillToDiskBounceBufferSize") {
          SpillFramework.shutdown()
          try {
            val sc = new SparkConf
            sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, hostSpillStorageSize)
            sc.set(RapidsConf.CHUNKED_PACK_BOUNCE_BUFFER_SIZE.key, chunkedPackBounceBufferSize)
            sc.set(RapidsConf.SPILL_TO_DISK_BOUNCE_BUFFER_SIZE.key, spillToDiskBounceBufferSize)
            sc.set(RapidsConf.OFF_HEAP_LIMIT_ENABLED.key, "false")
            SpillFramework.initialize(new RapidsConf(sc))

            val (expectedTable, _) = buildTable()
            withResource(expectedTable) { _ =>
              val (handle, _) = addSpillableTableToFramework()
              val diskStore = SpillFramework.stores.diskStore
              val hostStore = SpillFramework.stores.hostStore
              val deviceStore = SpillFramework.stores.deviceStore
              withResource(handle) { _ =>
                assertResult(1)(deviceStore.numHandles)
                assertResult(0)(diskStore.numHandles)
                assertResult(0)(hostStore.numHandles)

                deviceStore.spill(handle.approxSizeInBytes)
                hostStore.spill(handle.approxSizeInBytes)

                assertResult(0)(deviceStore.numHandles)
                assertResult(0)(hostStore.numHandles)
                assertResult(1)(diskStore.numHandles)

                val diskHandle = handle.host.flatMap(_.disk).get
                val path = diskStore.getFile(diskHandle.blockId)
                assert(path.exists)
                withResource(handle.materialize()) { actualTable =>
                  TestUtils.compareTables(expectedTable, actualTable)
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
}
