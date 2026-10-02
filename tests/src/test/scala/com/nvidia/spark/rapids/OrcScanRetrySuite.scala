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

import ai.rapids.cudf.{ColumnVector, DType, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{GpuTimeZoneDB, RmmSpark}

import org.apache.spark.sql.types.{
  CharType, DecimalType, LongType, StringType, StructField, StructType, TimestampType, VarcharType}

class OrcScanRetrySuite extends RmmSparkRetrySuiteBase {

  private val timestampSchema = StructType(Seq(StructField("a", TimestampType)))
  private val longSchema = StructType(Seq(StructField("a", LongType)))

  override def beforeEach(): Unit = {
    super.beforeEach()
    GpuTimeZoneDB.cacheDatabase()
  }

  override def afterEach(): Unit = {
    try {
      GpuTimeZoneDB.shutdown()
    } finally {
      super.afterEach()
    }
  }

  private def injectGpuRetryOom(): Unit = {
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
  }

  private def assertRetrySucceeds(table: Table, tableSchema: StructType): Unit = {
    injectGpuRetryOom()
    withResource(GpuOrcScan.rebaseAndEvolveSchemaWithRetryAndClose(
        table, tableSchema, timestampSchema, isSchemaCaseSensitive = true,
        writerTimezone = ZoneId.of("UTC"), writerUsedProlepticGregorian = true)) { result =>
      assertResult(1)(result.getRowCount)
      assertResult(DType.TIMESTAMP_MICROSECONDS)(result.getColumn(0).getType)
    }
  }

  test("ORC timestamp rebase is retried on OOM") {
    val table = withResource(ColumnVector.fromLongs(0L)) { longs =>
      withResource(longs.castTo(DType.TIMESTAMP_MICROSECONDS)) { timestamps =>
        new Table(timestamps)
      }
    }
    assertRetrySucceeds(table, timestampSchema)
  }

  test("ORC integer-to-timestamp schema evolution is retried on OOM") {
    val table = withResource(ColumnVector.fromLongs(0L)) { longs =>
      new Table(longs)
    }
    assertRetrySucceeds(table, longSchema)
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
  }

  Seq(
    ("CHAR", CharType(6), "abc   ", "abc"),
    ("VARCHAR", VarcharType(6), "abc", "abc")
  ).foreach { case (typeName, tableType, input, expected) =>
    test(s"ORC $typeName schema evolution uses STRING for retry") {
      val table = withResource(ColumnVector.fromStrings(input)) { strings =>
        new Table(strings)
      }
      val tableSchema = StructType(Seq(StructField("a", tableType)))
      val readSchema = StructType(Seq(StructField("a", StringType)))

      injectGpuRetryOom()
      withResource(GpuOrcScan.rebaseAndEvolveSchemaWithRetryAndClose(
          table, tableSchema, readSchema, isSchemaCaseSensitive = true,
          writerTimezone = ZoneId.of("UTC"), writerUsedProlepticGregorian = true)) { result =>
        withResource(result.getColumn(0).copyToHost()) { host =>
          assertResult(expected)(host.getJavaString(0))
        }
      }
    }
  }
}
