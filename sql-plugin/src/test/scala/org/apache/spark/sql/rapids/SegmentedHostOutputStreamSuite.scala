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
package org.apache.spark.sql.rapids

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.GatheringByteChannel

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import ai.rapids.cudf.{DefaultHostMemoryAllocator, HostMemoryAllocator, HostMemoryBuffer}
import com.nvidia.spark.rapids.{RapidsConf, SegmentedHostOutputStream}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.SegmentedHostOutputStream.OwnedBuffer
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.CloseShieldOutputStream
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.storage.ShuffleBlockId

class SegmentedHostOutputStreamSuite extends AnyFunSuite {
  private val segmentKey = "spark.rapids.shuffle.multiThreaded.writer.buffer.segmentSize"

  test("segment configuration defaults to 128 KiB and requires 8 KiB multiples up to 32 MiB") {
    assert(new RapidsConf(Map.empty[String, String]).shuffleMultiThreadedWriterBufferSegmentSize ==
      128 * 1024)
    for ((value, expected) <- Seq(
        "8192" -> 8192, "24k" -> 24576, "32m" -> 33554432)) {
      assert(new RapidsConf(Map(segmentKey -> value)).shuffleMultiThreadedWriterBufferSegmentSize ==
        expected)
    }
    for (value <- Seq("-8k", "0", "8191", "12k", "33m", "5x")) {
      intercept[IllegalArgumentException] {
        new RapidsConf(Map(segmentKey -> value)).shuffleMultiThreadedWriterBufferSegmentSize
      }
    }
  }

  test("mixed writes and invalid ranges preserve data across segment boundaries") {
    val bytes = Array.tabulate[Byte](33)(_.toByte)
    val segmentSize = 7
    withResource(new SegmentedHostOutputStream(segmentSize)) { out =>
      out.write(bytes, 0, 0)
      out.write(bytes, 0, segmentSize)
      for ((offset, length) <- Seq((-1, 1), (0, -1), (Int.MaxValue, 1))) {
        intercept[IndexOutOfBoundsException](out.write(bytes, offset, length))
      }
      intercept[NullPointerException](out.write(null, 0, 0))
      out.write(bytes(segmentSize).toInt)
      out.write(bytes, segmentSize + 1, bytes.length - segmentSize - 1)
      withResource(out.takeBuffer()) { owned =>
        val actual = new ByteArrayOutputStream()
        owned.writeTo(actual)
        assert(actual.toByteArray.sameElements(bytes))
      }
    }
    for (size <- Seq(0, -1, Int.MaxValue)) {
      intercept[IllegalArgumentException](new SegmentedHostOutputStream(size))
    }
  }

  test("gathering writes preserve record tails across short writes and batch boundaries") {
    val bytes = Array.tabulate[Byte](11)(_.toByte)
    val tail = Array[Byte](42)
    val result = new ByteArrayOutputStream()
    var largestVector = 0
    val channel = new GatheringByteChannel {
      override def write(srcs: Array[ByteBuffer], offset: Int, length: Int): Long = {
        largestVector = math.max(largestVector, length)
        var written = 0
        var index = offset
        while (index < offset + length && written < 5) {
          val buf = srcs(index)
          while (buf.hasRemaining && written < 5) {
            result.write(buf.get())
            written += 1
          }
          index += 1
        }
        written.toLong
      }
      override def write(srcs: Array[ByteBuffer]): Long = write(srcs, 0, srcs.length)
      override def write(src: ByteBuffer): Int = fail("Expected a gathering write")
      override def isOpen: Boolean = true
      override def close(): Unit = ()
    }
    def record(data: Array[Byte]): OwnedBuffer = {
      withResource(new SegmentedHostOutputStream(7)) { stream =>
        stream.write(data)
        stream.takeBuffer()
      }
    }
    withResource(record(bytes)) { first =>
      withResource(record(tail)) { second =>
        val records = Iterator.fill(37)(Iterator(first, second)).flatten
        OwnedBuffer.writeTo(channel, records.asJava)
      }
    }
    val expected = Array.fill(37)(bytes ++ tail).flatten
    assert(result.toByteArray.sameElements(expected))
    assert(largestVector > 2) // A vector spans more than the two segments in the first record.
  }

  // Exercise each codec's framing/close behavior, plus Spark's shared encryption wrapper.
  for ((codec, encrypted) <- Seq(
      ("lz4", true), ("lzf", false), ("snappy", false), ("zstd", false))) {
    test(s"$codec shuffle roundtrip across segments with encryption=$encrypted") {
      val conf = new SparkConf(false).set("spark.shuffle.compress", "true")
        .set("spark.io.compression.codec", codec)
      val key = if (encrypted) Some(Array.tabulate[Byte](16)(_.toByte)) else None
      val manager = new SerializerManager(new JavaSerializer(conf), conf, key)
      val block = ShuffleBlockId(1, 2, 3)
      val segmentSize = 8192
      val bytes = new Array[Byte](2 * segmentSize + 103)
      new Random(19).nextBytes(bytes)
      val serialized = new ByteArrayOutputStream()
      withResource(new SegmentedHostOutputStream(segmentSize)) { buffer =>
        val encoded = manager.wrapStream(block, new CloseShieldOutputStream(buffer))
        withResource(encoded)(_.write(bytes))
        withResource(buffer.takeBuffer())(_.writeTo(serialized))
      }
      withResource(manager.wrapStream(block, new ByteArrayInputStream(serialized.toByteArray))) {
        input => assert(IOUtils.toByteArray(input).sameElements(bytes))
      }
    }
  }

  test("handoff retains each original host allocation until its final owner closes") {
    val allocator = DefaultHostMemoryAllocator.get
    val allocated = ArrayBuffer.empty[HostMemoryBuffer]
    var failNextAllocation = false
    DefaultHostMemoryAllocator.set(new HostMemoryAllocator {
      override def allocate(bytes: Long): HostMemoryBuffer = fail("expected a pageable allocation")
      override def allocate(bytes: Long, preferPinned: Boolean): HostMemoryBuffer = {
        if (failNextAllocation) throw new OutOfMemoryError("injected host allocation failure")
        assert(!preferPinned && bytes == 7)
        val segment = allocator.allocate(bytes, false)
        allocated += segment
        segment
      }
    })
    try {
      for (length <- Seq(0, 3, 7, 33)) {
        allocated.clear()
        val bytes = Array.tabulate[Byte](length)(_.toByte)
        withResource(new SegmentedHostOutputStream(7)) { stream =>
          stream.write(bytes)
          val segmentCount = (length + 6) / 7
          withResource(stream.takeBuffer()) { published =>
            withResource(published.takeBuffer()) { owned =>
              stream.close()
              published.close()
              intercept[IllegalStateException](stream.write(1))
              intercept[IllegalStateException](published.takeBuffer())
              assert(allocated.size == segmentCount)
              assert(allocated.forall(_.getRefCount == 1))
              assert(owned.getCapacity == segmentCount * 7 && owned.getCount == length)
              val actual = new Array[Byte](length + 2)
              owned.read(0, actual, 1, length)
              assert(actual.slice(1, length + 1).sameElements(bytes))
              intercept[IndexOutOfBoundsException](owned.read(length, actual, 0, 1))
              if (length > 0) {
                // A change through the original allocation is visible through the final owner.
                allocated.last.setByte((length - 1) % 7, 42.toByte)
                owned.read(length - 1, actual, 0, 1)
                assert(actual(0) == 42)
                val failure = new IOException("injected destination failure")
                val broken = new OutputStream {
                  override def write(value: Int): Unit = throw failure
                }
                assert(intercept[IOException](owned.writeTo(broken)) eq failure)
              }
            }
          }
        }
        assert(allocated.forall(_.getRefCount == 0))
      }
      allocated.clear()
      intercept[OutOfMemoryError] {
        withResource(new SegmentedHostOutputStream(7)) { stream =>
          stream.write(new Array[Byte](14))
          failNextAllocation = true
          stream.write(1)
        }
      }
      assert(allocated.size == 2 && allocated.forall(_.getRefCount == 0))
    } finally {
      DefaultHostMemoryAllocator.set(allocator)
    }
  }
}
