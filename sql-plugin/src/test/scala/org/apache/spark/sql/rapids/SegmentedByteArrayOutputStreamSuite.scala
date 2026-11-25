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

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.nvidia.spark.rapids.{RapidsConf, SegmentedByteArrayOutputStream}
import com.nvidia.spark.rapids.Arm.withResource
import org.apache.commons.io.IOUtils
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.storage.ShuffleBlockId

class SegmentedByteArrayOutputStreamSuite extends AnyFunSuite {
  private val segmentKey = "spark.rapids.shuffle.multiThreaded.writer.buffer.segmentSize"

  test("segment configuration defaults to 128 KiB and accepts zero or 8 KiB through 32 MiB") {
    assert(new RapidsConf(Map.empty[String, String]).shuffleMultiThreadedWriterBufferSegmentSize ==
      128 * 1024)
    for ((value, expected) <- Seq(
        "0" -> 131072, "8192" -> 8192, "8193" -> 8193, "128k" -> 131072, "32m" -> 33554432)) {
      assert(new RapidsConf(Map(segmentKey -> value)).shuffleMultiThreadedWriterBufferSegmentSize ==
        expected)
    }
    for (value <- Seq("-1", "5k", "8191", "33554433", "1g", "5x")) {
      intercept[IllegalArgumentException] {
        new RapidsConf(Map(segmentKey -> value)).shuffleMultiThreadedWriterBufferSegmentSize
      }
    }
  }

  test("mixed writes and invalid ranges preserve data across segment boundaries") {
    val bytes = Array.tabulate[Byte](33)(_.toByte)
    for (segmentSize <- Seq(1, 7)) {
      val out = new SegmentedByteArrayOutputStream(segmentSize)
      out.write(bytes, 0, 0)
      assert(out.getCapacity == 0)
      out.write(bytes, 0, segmentSize)
      assert(out.getCapacity == segmentSize)
      for ((offset, length) <- Seq((-1, 1), (0, -1), (32, 2), (Int.MaxValue, 1))) {
        intercept[IndexOutOfBoundsException](out.write(bytes, offset, length))
      }
      intercept[NullPointerException](out.write(null, 0, 0))
      intercept[IllegalArgumentException](out.writeTo(out))
      out.write(bytes(segmentSize).toInt)
      out.write(bytes, segmentSize + 1, bytes.length - segmentSize - 1)
      out.close()
      assert(out.getCount == bytes.length)
      assert(out.getCapacity == ((bytes.length.toLong - 1) / segmentSize + 1) * segmentSize)
      // Closing a codec closes its underlying stream. The merger still needs to read it.
      for (_ <- 0 until 2) {
        val actual = new ByteArrayOutputStream()
        out.writeTo(actual)
        assert(actual.toByteArray.sameElements(bytes))
      }
    }
    for (size <- Seq(0, -1, Int.MaxValue)) {
      intercept[IllegalArgumentException](new SegmentedByteArrayOutputStream(size))
    }
  }

  // Exercise each codec's framing/close behavior, plus Spark's shared encryption wrapper.
  for ((codec, encrypted) <- Seq(
      ("lz4", false), ("lzf", false), ("snappy", false), ("zstd", false), ("lz4", true))) {
    test(s"$codec shuffle roundtrip across segments with encryption=$encrypted") {
      val conf = new SparkConf(false).set("spark.shuffle.compress", "true")
        .set("spark.io.compression.codec", codec)
      val key = if (encrypted) Some(Array.tabulate[Byte](16)(_.toByte)) else None
      val manager = new SerializerManager(new JavaSerializer(conf), conf, key)
      val block = ShuffleBlockId(1, 2, 3)
      val bytes = new Array[Byte](3 * 128 * 1024 + 103)
      new Random(19).nextBytes(bytes)
      val buffer = new SegmentedByteArrayOutputStream(128 * 1024)
      withResource(manager.wrapStream(block, buffer))(_.write(bytes))
      val serialized = new ByteArrayOutputStream()
      withResource(buffer.takeBuffer())(_.writeTo(serialized))
      withResource(manager.wrapStream(block, new ByteArrayInputStream(serialized.toByteArray))) {
        input => assert(IOUtils.toByteArray(input).sameElements(bytes))
      }
    }
  }

  test("publication trims every partial tail and ownership transfer preserves the arrays") {
    def capture(write: OutputStream => Unit): ArrayBuffer[Array[Byte]] = {
      val arrays = ArrayBuffer.empty[Array[Byte]]
      write(new OutputStream {
        override def write(value: Int): Unit = fail("unexpected single-byte write")
        override def write(data: Array[Byte], start: Int, length: Int): Unit = {
          assert(start == 0)
          arrays += data
        }
      })
      arrays
    }
    // Empty, partial, exact-boundary, and multiple-segment records share the same contract.
    for (length <- Seq(0, 3, 7, 33)) {
      val stream = new SegmentedByteArrayOutputStream(7)
      val bytes = Array.tabulate[Byte](length)(_.toByte)
      val first = math.min(7, length)
      stream.write(bytes, 0, first)
      val initial = capture(stream.writeTo)
      stream.write(bytes, first, length - first)
      val before = capture(stream.writeTo)
      assert(initial.zip(before).forall { case (a, b) => a eq b },
        "appending segments must not replace existing arrays")
      stream.close() // codec close leaves the payload available
      val published = stream.takeBuffer()
      val trimmed = capture(published.writeTo)
      val owned = published.takeBuffer()
      try {
        intercept[IllegalStateException](published.takeBuffer())
        intercept[IllegalStateException](published.writeTo(new ByteArrayOutputStream()))
        published.close() // Future cleanup must not clear the consumer's arrays.
        val after = capture(owned.writeTo)
        assert(before.size == after.size)
        assert(before.take(length / 7).zip(after).forall { case (a, b) => a eq b })
        assert(trimmed.zip(after).forall { case (a, b) => a eq b })
        assert(after.map(_.length).sum == length, "retained arrays must have no unused capacity")
        if (length % 7 != 0) {
          assert(!(before.last eq after.last))
          assert(after.last.length == length % 7)
        }
        assert(owned.getCount == length)
        if (length > 0) {
          val failure = new IOException("injected destination failure")
          val broken = new OutputStream {
            override def write(value: Int): Unit = throw failure
            override def close(): Unit = fail("writeTo must not close its destination")
          }
          assert(intercept[IOException](owned.writeTo(broken)) eq failure)
        }
        intercept[IllegalStateException](stream.write(1))
        intercept[IllegalStateException](stream.write(new Array[Byte](0)))
        intercept[IllegalStateException](stream.writeTo(new ByteArrayOutputStream()))
        intercept[IllegalStateException](stream.takeBuffer())
        val actual = new Array[Byte](length + 2)
        owned.read(0, actual, 1, length)
        assert(actual.slice(1, length + 1).sameElements(bytes))
        if (length > 3) {
          owned.read(3, actual, 0, length - 3)
          assert(actual.take(length - 3).sameElements(bytes.drop(3)))
        }
        intercept[IndexOutOfBoundsException](owned.read(length, actual, 0, 1))
      } finally {
        published.close()
        owned.close()
        owned.close()
      }
      intercept[IllegalStateException](owned.read(0, new Array[Byte](1), 0, 1))
    }
  }

}
