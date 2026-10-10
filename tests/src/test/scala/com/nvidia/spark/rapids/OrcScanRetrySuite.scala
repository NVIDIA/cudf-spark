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

package com.nvidia.spark.rapids

import java.math.BigInteger
import java.time.ZoneId
import java.util.TimeZone

import ai.rapids.cudf.{ColumnVector, DType, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{GpuTimeZoneDB, RmmSpark}

import org.apache.spark.sql.types.{
  CharType, DecimalType, LongType, StringType, StructField, StructType, TimestampType, VarcharType}

class OrcScanRetrySuite extends RmmSparkRetrySuiteBase {

  private val timestampSchema = StructType(Seq(StructField("a", TimestampType)))
  private val longSchema = StructType(Seq(StructField("a", LongType)))
  private val shanghaiZone = ZoneId.of("Asia/Shanghai")
  private val decodedShanghaiTimestampUs = 21087883873L
  private val expectedShanghaiTimestampUs = -7713116127L
  private val expectedShanghaiIntegerTimestampUs = -28800000000L

  override def beforeEach(): Unit = {
    super.beforeEach()
    GpuTimeZoneDB.cacheDatabase()
  }

  override def afterEach(): Unit = {
    try {
      GpuTimeZoneDB.shutdown()
    } finally {
      RmmSpark.getAndResetNumRetryThrow(/*taskId*/ 1)
      super.afterEach()
    }
  }

  private def injectGpuRetryOom(): Unit = {
    RmmSpark.getAndResetNumRetryThrow(/*taskId*/ 1)
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
  }

  private def assertGpuRetryOccurred(): Unit = {
    val retryCount = RmmSpark.getAndResetNumRetryThrow(/*taskId*/ 1)
    assert(retryCount > 0, s"expected at least one retry but saw $retryCount")
  }

  private def withDefaultTimeZone[T](zone: ZoneId)(body: => T): T = {
    val originalTimeZone = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(zone))
      body
    } finally {
      TimeZone.setDefault(originalTimeZone)
    }
  }

  private def assertRetrySucceeds(
      table: Table,
      tableSchema: StructType,
      expectedTimestampUs: Long): Unit = {
    injectGpuRetryOom()
    withResource(GpuOrcScan.rebaseAndEvolveSchemaWithRetryAndClose(
        table, tableSchema, timestampSchema, isSchemaCaseSensitive = true,
        writerTimezone = shanghaiZone, writerUsedProlepticGregorian = true)) { result =>
      assertResult(1)(result.getRowCount)
      assertResult(DType.TIMESTAMP_MICROSECONDS)(result.getColumn(0).getType)
      withResource(result.getColumn(0).copyToHost()) { host =>
        assertResult(expectedTimestampUs)(host.getLong(0))
      }
    }
    assertGpuRetryOccurred()
  }

  test("ORC timestamp rebase is retried on OOM") {
    withDefaultTimeZone(shanghaiZone) {
      val table = withResource(ColumnVector.fromLongs(decodedShanghaiTimestampUs)) { longs =>
        withResource(longs.castTo(DType.TIMESTAMP_MICROSECONDS)) { timestamps =>
          new Table(timestamps)
        }
      }
      assertRetrySucceeds(table, timestampSchema, expectedShanghaiTimestampUs)
    }
  }

  test("ORC integer-to-timestamp schema evolution is retried on OOM") {
    withDefaultTimeZone(shanghaiZone) {
      val table = withResource(ColumnVector.fromLongs(0L)) { longs =>
        new Table(longs)
      }
      assertRetrySucceeds(table, longSchema, expectedShanghaiIntegerTimestampUs)
    }
  }

  test("ORC decimal schema evolution uses the physical decimal type for retry") {
    val table = withResource(ColumnVector.decimalFromBigInt(-2, BigInteger.valueOf(123))) {
      decimal => new Table(decimal)
    }
    val tableSchema = StructType(Seq(StructField("a", DecimalType(9, 2))))
    val readSchema = StructType(Seq(StructField("a", DecimalType(38, 6))))

    injectGpuRetryOom()
    withResource(GpuOrcScan.rebaseAndEvolveSchemaWithRetryAndClose(
        table, tableSchema, readSchema, isSchemaCaseSensitive = true,
        writerTimezone = ZoneId.of("UTC"), writerUsedProlepticGregorian = true)) { result =>
      assertResult(DType.create(DType.DTypeEnum.DECIMAL128, -6))(result.getColumn(0).getType)
    }
    assertGpuRetryOccurred()
  }

  test("ORC CHAR and VARCHAR schema evolution uses STRING for retry") {
    val table = withResource(ColumnVector.fromStrings("abc   ")) { charColumn =>
      withResource(ColumnVector.fromStrings("abc")) { varcharColumn =>
        new Table(charColumn, varcharColumn)
      }
    }
    val tableSchema = StructType(Seq(
      StructField("char", CharType(6)),
      StructField("varchar", VarcharType(6))))
    val readSchema = StructType(Seq(
      StructField("char", StringType),
      StructField("varchar", StringType)))

    injectGpuRetryOom()
    withResource(GpuOrcScan.rebaseAndEvolveSchemaWithRetryAndClose(
        table, tableSchema, readSchema, isSchemaCaseSensitive = true,
        writerTimezone = ZoneId.of("UTC"), writerUsedProlepticGregorian = true)) { result =>
      assertResult(2)(result.getNumberOfColumns)
      withResource(result.getColumn(0).copyToHost()) { charHost =>
        assertResult("abc")(charHost.getJavaString(0))
      }
      withResource(result.getColumn(1).copyToHost()) { varcharHost =>
        assertResult("abc")(varcharHost.getJavaString(0))
      }
    }
    assertGpuRetryOccurred()
  }
}
