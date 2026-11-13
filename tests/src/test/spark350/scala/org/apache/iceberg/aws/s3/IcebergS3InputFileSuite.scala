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

/*** spark-rapids-shim-json-lines
{"spark":["350"],"carryForward":"all"}
{"spark":["400","401","402","403","404"],"carryForward":"none"}
{"spark":["411"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.aws.s3

import java.io.IOException
import java.util.Collections

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.IcebergS3RangeCopier
import com.nvidia.spark.rapids.IcebergS3RangeCopier.IcebergS3Client
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange
import org.apache.iceberg.hadoop.HadoopMetricsContext
import org.apache.iceberg.io.{FileIO, FileIOMetricsContext, InputFile}
import org.apache.iceberg.metrics.{Counter, DefaultMetricsContext}
import org.apache.iceberg.metrics.MetricsContext.Unit
import org.mockito.Mockito.{mock, mockStatic, never, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.rapids.execution.TrampolineUtil

class IcebergS3InputFileSuite extends AnyFunSuite with Matchers {

  private def newInputFile(
      readBytes: Counter,
      delegate: InputFile = mock(classOf[InputFile])): IcebergS3InputFile = {
    val constructor = classOf[IcebergS3InputFile].getDeclaredConstructor(
      classOf[InputFile],
      classOf[String],
      classOf[String],
      classOf[IcebergS3Client],
      classOf[Counter])
    constructor.setAccessible(true)
    constructor.newInstance(
      delegate,
      "bucket",
      "key",
      mock(classOf[IcebergS3Client]),
      readBytes)
  }

  test("known file sizes are forwarded to Iceberg FileIO") {
    val delegate = mock(classOf[FileIO])
    val input = mock(classOf[InputFile])
    when(delegate.newInputFile("s3://bucket/key", 123L)).thenReturn(input)
    val file = new IcebergFileIO(delegate).newInputFile("s3://bucket/key", 123L)
    file.getDelegate shouldBe input
    verify(delegate).newInputFile("s3://bucket/key", 123L)
    verify(delegate, never()).newInputFile("s3://bucket/key")
  }

  test("metadata streams buffer bounded reads and support backward seeks and EOF") {
    val data = Array.tabulate[Byte](150000)(i => (i % 251).toByte)
    val delegate = mock(classOf[InputFile])
    when(delegate.getLength).thenReturn(data.length.toLong)
    val readBytes = new DefaultMetricsContext().counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES)
    val ranges = ArrayBuffer.empty[CopyRange]
    val buffers = ArrayBuffer.empty[HostMemoryBuffer]
    val copier = mockStatic(classOf[IcebergS3RangeCopier], (invocation: InvocationOnMock) => {
      val output = invocation.getArgument[HostMemoryBuffer](1)
      val range = invocation.getArgument[java.util.List[CopyRange]](4).get(0)
      ranges += range
      buffers += output
      output.setBytes(0, data, range.getInputOffset, range.getLength)
      Long.box(range.getLength)
    })
    try {
      val input = newInputFile(readBytes, delegate)
      withResource(input.open()) { stream =>
        (0 until 100).foreach(i => stream.read() shouldBe (data(i) & 0xff))
        ranges.size shouldBe 1
        stream.seek(7)
        stream.read() shouldBe (data(7) & 0xff)
        ranges.size shouldBe 1

        stream.seek(90000)
        stream.read() shouldBe (data(90000) & 0xff)
        stream.seek(80000)
        stream.read() shouldBe (data(80000) & 0xff)
        ranges.size shouldBe 2
        stream.seek(3)
        stream.read() shouldBe (data(3) & 0xff)
        ranges.size shouldBe 3

        stream.seek(data.length - 5)
        val tail = new Array[Byte](10)
        stream.read(tail, 2, 8) shouldBe 5
        tail.slice(2, 7) shouldBe data.takeRight(5)
        stream.getPos shouldBe data.length
        stream.read() shouldBe -1
        stream.read(tail, 0, 0) shouldBe 0
        stream.seek(data.length + 10L)
        stream.read() shouldBe -1
        intercept[IOException](stream.seek(-1))
        intercept[IndexOutOfBoundsException](stream.read(tail, 2, 9))
        ranges.forall(r => r.getLength <= 65536 &&
          r.getInputOffset + r.getLength <= data.length) shouldBe true
        readBytes.value() shouldBe ranges.map(_.getLength).sum
      }
      buffers.distinct.foreach(_.getRefCount shouldBe 0)
      verify(delegate, never()).newStream()
    } finally {
      copier.close()
    }
  }

  test("failed metadata refills invalidate old bytes and close releases the buffer once") {
    val delegate = mock(classOf[InputFile])
    when(delegate.getLength).thenReturn(200000L)
    val readBytes = new DefaultMetricsContext().counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES)
    val buffer = mock(classOf[HostMemoryBuffer])
    val allocator = mockStatic(classOf[HostMemoryBuffer], (_: InvocationOnMock) => buffer)
    var failRead = false
    var attempts = 0
    val copier = mockStatic(classOf[IcebergS3RangeCopier], (invocation: InvocationOnMock) => {
      attempts += 1
      if (failRead) throw new IOException("injected read failure")
      Long.box(invocation.getArgument[java.util.List[CopyRange]](4).get(0).getLength)
    })
    try {
      val stream = newInputFile(readBytes, delegate).open()
      withResource(stream) { _ =>
        stream.read() shouldBe 0
        failRead = true
        stream.seek(100000)
        intercept[IOException](stream.read())
        stream.getPos shouldBe 100000L
        stream.seek(0)
        intercept[IOException](stream.read())
        attempts shouldBe 3
        readBytes.value() shouldBe 65536L
      }
      stream.close()
      verify(buffer).close()
      intercept[IOException](stream.read())
      intercept[IOException](stream.seek(0))
    } finally {
      copier.close()
      allocator.close()
    }
  }

  test("read bytes counter updates Hadoop statistics used by Spark") {
    val metrics = new HadoopMetricsContext("s3")
    val delegate = mock(classOf[S3InputFile])
    when(delegate.uri()).thenReturn(new S3URI("s3://bucket/key"))
    when(delegate.metrics()).thenReturn(metrics)

    IcebergS3InputFileAccess.s3BucketAndKey(delegate) should contain theSameElementsInOrderAs
      Array("bucket", "key")
    val readBytes = IcebergS3InputFileAccess.readBytesCounter(delegate)
    val sparkBytesRead = TrampolineUtil.getFSBytesReadOnThreadCallback()

    readBytes.increment(17L)

    sparkBytesRead() shouldBe 17L
  }

  test("successful PerfIO reads count the bytes delivered by the copier") {
    val metrics = new DefaultMetricsContext
    val readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES)
    val inputFile = newInputFile(readBytes)
    val output = mock(classOf[HostMemoryBuffer])
    val ranges = Collections.singletonList(new CopyRange(0L, 100L, 0L))
    val copier = mockStatic(classOf[IcebergS3RangeCopier], (invocation: InvocationOnMock) => {
      invocation.getMethod.getName match {
        case "copyToHMB" => Long.box(23L)
        case "copyTailToHMB" => Long.box(11L)
        case _ => null
      }
    })
    try {
      inputFile.readVectored(output, ranges)
      readBytes.value() shouldBe 23L

      inputFile.readTail(100L, output)
      readBytes.value() shouldBe 34L

      inputFile.readTail(0L, output)
      readBytes.value() shouldBe 34L
    } finally {
      copier.close()
    }
  }

  test("failed PerfIO reads do not update read bytes") {
    val metrics = new DefaultMetricsContext
    val readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES)
    val inputFile = newInputFile(readBytes)
    val output = mock(classOf[HostMemoryBuffer])
    val ranges = Collections.singletonList(new CopyRange(0L, 100L, 0L))
    val copier = mockStatic(classOf[IcebergS3RangeCopier], (invocation: InvocationOnMock) => {
      invocation.getMethod.getName match {
        case "copyToHMB" | "copyTailToHMB" => throw new IOException("read failed")
        case _ => null
      }
    })
    try {
      intercept[IOException](inputFile.readVectored(output, ranges))
      intercept[IOException](inputFile.readTail(100L, output))
      readBytes.value() shouldBe 0L
    } finally {
      copier.close()
    }
  }
}
