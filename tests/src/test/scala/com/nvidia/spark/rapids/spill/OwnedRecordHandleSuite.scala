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

import java.io.IOException
import java.nio.file.Files

import com.nvidia.spark.rapids.{HostAlloc, RapidsConf, SegmentedByteArrayOutputStream}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.SegmentedByteArrayOutputStream.OwnedBuffer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class OwnedRecordHandleSuite extends AnyFunSuite with BeforeAndAfterEach {
  override def beforeEach(): Unit = {
    super.beforeEach()
    SpillFramework.initialize(new RapidsConf(Map.empty[String, String]))
  }
  override def afterEach(): Unit = {
    try {
      SpillFramework.shutdown()
    } finally {
      super.afterEach()
    }
  }
  private def record(data: Array[Byte], segment: Int = 7): OwnedBuffer = {
    val result = new SegmentedByteArrayOutputStream(segment)
    result.write(data)
    result.close()
    result.takeBuffer()
  }
  private def create(file: java.io.File, max: Long = 1048576) = {
    SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = math.min(1024L, max), maxBufferSize = max,
      memoryThreshold = 1.0, spillFile = file, retainRecords = true,
      capacityHintProvider = Some((_, _) => fail("Owned records must not request native capacity")))
  }

  test("records read across segment tails and spill with exact capacity accounting") {
    val segment = 128 * 1024
    val file = Files.createTempFile("owned-record", ".bin").toFile
    val a = Array.tabulate[Byte](segment + 3)(_.toByte)
    val b = Array.tabulate[Byte](segment * 2 + 1)(i => (i * 17).toByte)
    val expected = a ++ b
    withResource(create(file)) { handle =>
      assert(handle.host.get.capacity == 0)
      val first = record(a, segment)
      val second = record(b, segment)
      handle.writeRecord(first)
      handle.writeRecord(record(Array.emptyByteArray, segment))
      handle.writeRecord(second)
      first.close()
      second.close()
      assert(handle.host.get.capacity == expected.length)
      handle.finishWrite()
      for (offset <- Seq(0, a.length - 2, a.length, expected.length - 1)) {
        val actual = new Array[Byte](expected.length - offset)
        assert(handle.readAt(offset, actual, 0, actual.length) == actual.length)
        assert(actual.sameElements(expected.drop(offset)))
      }
      val prefix = new Array[Byte](a.length - 1)
      assert(handle.read(prefix, 0, prefix.length) == prefix.length)
      assert(prefix.sameElements(expected.take(prefix.length)))
      assert(handle.spillForCommit() == expected.length)
      assert(Files.readAllBytes(file.toPath).sameElements(expected))
      val suffix = new Array[Byte](expected.length - prefix.length)
      assert(handle.read(suffix, 0, suffix.length) == suffix.length)
      assert(suffix.sameElements(expected.drop(prefix.length)))
    }
    assert(!file.exists())
  }

  test("mixed writes ignore native capacity and isolate the caller's array") {
    withResource(create(Files.createTempFile("owned-limit", ".bin").toFile, max = 14)) { h =>
      val bytes = Array.tabulate[Byte](9)(_.toByte)
      val suffix = Array.fill[Byte](6)(42)
      h.writeRecord(record(bytes))
      h.write(suffix, 0, suffix.length)
      suffix(0) = 99
      h.write(43)
      h.writeRecord(record(Array[Byte](43)))
      assert(!h.isSpilled)
      assert(h.host.get.capacity > 14)
      assert(h.getExpansionCount == 0)
      h.finishWrite()
      val actual = new Array[Byte](17)
      assert(h.read(actual, 0, actual.length) == actual.length)
      assert(actual.sameElements(bytes ++ Array.fill[Byte](6)(42) ++ Array[Byte](43, 43)))
    }
  }

  for (retain <- Seq(false, true)) {
    test(s"native pressure applies only to contiguous native buffers: retain=$retain") {
      HostAlloc.initialize(4096)
      try {
        withResource(SpillablePartialFileHandle.createMemoryWithSpill(
            initialCapacity = 8, maxBufferSize = 32, memoryThreshold = 1e-20,
            spillFile = Files.createTempFile("owned-pressure", ".bin").toFile,
            retainRecords = retain)) { h =>
          h.writeRecord(record(Array[Byte](1, 2, 3, 4), segment = 4))
          assert(SpillFramework.stores.hostStore.totalSize == (if (retain) 0 else 8))
          withResource(HostAlloc.alloc(1024, false)) { _ =>
            assert(HostAlloc.getUsageRatio() > 1e-20)
            h.writeRecord(record(Array[Byte](5, 6), segment = 2))
            assert(!h.isSpilled)
            h.writeRecord(record(Array[Byte](7, 8, 9), segment = 3))
            assert(h.isSpilled == !retain)
            h.finishWrite()
            assert(SpillFramework.stores.hostStore.spill(Long.MaxValue) == 0)
            assert(h.isSpilled == !retain)
            val actual = new Array[Byte](9)
            assert(h.read(actual, 0, actual.length) == actual.length)
            assert(actual.sameElements(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9)))
          }
        }
        assert(SpillFramework.stores.hostStore.totalSize == 0)
      } finally {
        HostAlloc.initialize(-1)
      }
    }
  }

  test("failed explicit spill preserves records and permits retry") {
    val path = Files.createTempDirectory("owned-spill-failure")
    withResource(create(path.toFile, max = 3)) { h =>
      h.writeRecord(record(Array[Byte](1, 2, 3), segment = 3))
      h.writeRecord(record(Array[Byte](4)))
      h.finishWrite()
      intercept[IOException](h.spillForCommit())
      assert(!h.isSpilled)
      assert(SpillFramework.stores.hostStore.totalSize == 0)
      Files.delete(path)
      assert(h.spillForCommit() == 4)
      val actual = new Array[Byte](4)
      assert(h.read(actual, 0, 4) == 4)
      assert(actual.sameElements(Array[Byte](1, 2, 3, 4)))
    }
  }

  test("read lease keeps transferred records alive until the final consumer closes") {
    val h = create(Files.createTempFile("owned-lease", ".bin").toFile)
    h.writeRecord(record(Array[Byte](42)))
    h.finishWrite()
    h.acquireRead()
    try {
      h.close()
      val actual = new Array[Byte](1)
      assert(h.readAt(0, actual, 0, 1) == 1 && actual(0) == 42)
      intercept[IllegalStateException](h.writeRecord(record(Array[Byte](1))))
    } finally {
      h.releaseRead()
      h.close()
    }
  }
}
