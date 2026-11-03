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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.aws.s3

import java.io.IOException
import java.util.Collections

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.IcebergS3RangeCopier
import com.nvidia.spark.rapids.IcebergS3RangeCopier.IcebergS3Client
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange
import org.apache.iceberg.hadoop.HadoopMetricsContext
import org.apache.iceberg.io.{FileIOMetricsContext, InputFile}
import org.apache.iceberg.metrics.{Counter, DefaultMetricsContext}
import org.apache.iceberg.metrics.MetricsContext.Unit
import org.mockito.Mockito.{mock, mockStatic, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.rapids.execution.TrampolineUtil

class IcebergS3InputFileSuite extends AnyFunSuite with Matchers {

  private def newInputFile(readBytes: Counter): IcebergS3InputFile = {
    val constructor = classOf[IcebergS3InputFile].getDeclaredConstructor(
      classOf[InputFile],
      classOf[String],
      classOf[String],
      classOf[IcebergS3Client],
      classOf[Counter])
    constructor.setAccessible(true)
    constructor.newInstance(
      mock(classOf[InputFile]),
      "bucket",
      "key",
      mock(classOf[IcebergS3Client]),
      readBytes)
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
