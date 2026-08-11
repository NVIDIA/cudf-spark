/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
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

import java.io.File
import java.nio.file.Files
import java.util.Objects

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.shims.OrcStatisticShim
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil.fullyDelete
import org.apache.hadoop.fs.Path
import org.apache.orc._
import org.apache.orc.impl._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.tests.datagen.BigDataGenConsts._
import org.apache.spark.sql.tests.datagen.DBGen
import org.apache.spark.sql.types.{DateType, TimestampType}

class OrcSuite extends SparkQueryCompareTestSuite {
  test("Statistics tests for ORC files written by GPU") {
    assume(false, "blocked by cuDF issue: " +
        "https://github.com/rapidsai/cudf/issues/13793," +
        "https://github.com/rapidsai/cudf/issues/13837," +
        "https://github.com/rapidsai/cudf/issues/13817")
    val rowsNum: Long = 1024L * 1024L

    val testDataPath = Files.createTempDirectory("spark-rapids-orc-suite").toFile

    def writeTestDataOnCpu(): Unit = {
      withCpuSparkSession { spark =>
        // define table
        val gen = DBGen()
        gen.setDefaultValueRange(TimestampType, minTimestampForOrc, maxTimestampForOrc)
        gen.setDefaultValueRange(DateType, minDateIntForOrc, maxDateIntForOrc)
        val tab = gen.addTable("tab", statTestSchema, rowsNum)
        tab("c03").setNullProbability(0.5)
        tab("c08").setNullProbability(0.5)
        tab("c14").setNullProbability(0.5)

        // write to ORC file on CPU
        tab.toDF(spark).coalesce(1).write.mode("overwrite")
            .orc(testDataPath.getAbsolutePath)
      }
    }

    def getStats: SparkSession => OrcStat = { spark =>
      withTempPath { writePath =>
        // write to ORC file
        spark.read.orc(testDataPath.getAbsolutePath).coalesce(1)
            .write.mode("overwrite").orc(writePath.getAbsolutePath)

        // get Stats
        getStatsFromFile(writePath)
      }
    }

    try {
      // generate test data on CPU
      writeTestDataOnCpu()

      // write data and get stats on CPU
      val cpuStats = withCpuSparkSession(getStats)

      // write data and get stats on GPU
      ExecutionPlanCaptureCallback.startCapture()
      val gpuStats = withGpuSparkSession(getStats)
      val plan = ExecutionPlanCaptureCallback.getResultsWithTimeout()
      ExecutionPlanCaptureCallback.assertContains(plan(0), "GpuDataWritingCommandExec")

      // compare stats
      assertResult(cpuStats)(gpuStats)
    } finally {
      fullyDelete(testDataPath)
    }
  }

  /**
   * Find a orc file in orcDir and get the stats. It's similar to output of `orc-tool meta file`
   *
   * @param orcDir orc file directory
   * @return (encodingKind, dictionarySize) list for each columns passed in
   */
  private def getStatsFromFile(orcDir: File): OrcStat = {
    val orcFile = orcDir.listFiles(f => f.getName.endsWith(".orc"))(0)
    val p = new Path(orcFile.getCanonicalPath)
    val conf = OrcFile.readerOptions(new Configuration())

    // cdh321 and cdh330 use a lower ORC version, and the reader is not a AutoCloseable,
    // so use withResourceIfAllowed
    withResourceIfAllowed(OrcFile.createReader(p, conf)) { reader =>
      val calendar = if (reader.writerUsedProlepticGregorian) {
        "Proleptic Gregorian"
      } else {
        "Julian/Gregorian"
      }
      val stripeColumnStats = reader.getStripeStatistics.asScala.toArray.map { strip =>
        StripColumnStat(strip.getColumnStatistics.map { cs => ColumnStat(cs) })
      }

      val fileColumnStats = reader.getStatistics.map(cs => ColumnStat(cs))

      val stripeFooterStats = withResource(reader.rows().asInstanceOf[RecordReaderImpl]) { rows =>
        reader.getStripes.asScala.toArray.map { stripe =>
          val stripeFooter = rows.readStripeFooter(stripe)
          val streams = stripeFooter.getStreamsList.asScala.toArray.map(stream =>
            StreamStat(stream.getColumn, stream.getKind.name()))
          val encodings = stripeFooter.getColumnsList.asScala.toArray.map { encoding =>
            val encodingName = encoding.getKind.toString
            if (encodingName.startsWith("DICTIONARY")) {
              s"$encodingName[${encoding.getDictionarySize}]"
            } else {
              encodingName
            }
          }
          StripeFooterStat(streams, encodings)
        }
      }

      OrcStat(
        reader.getNumberOfRows,
        reader.getCompressionKind.toString,
        calendar,
        reader.getSchema.toString,
        stripeColumnStats,
        fileColumnStats,
        stripeFooterStats
      )
    }
  }

  case class OrcStat(
      rows: Long,
      compression: String,
      calendar: String,
      schema: String,
      stripeColumnStats: Seq[StripColumnStat],
      fileColumnStats: Seq[ColumnStat],
      stripeFooterStats: Seq[StripeFooterStat])

  case class StripColumnStat(columnStats: Seq[ColumnStat])

  /**
   * Refer to ColumnStatisticsImpl.java
   * Exclude the check for bytesOnDisk attribute
   */
  case class ColumnStat(cs: ColumnStatistics) {
    def equalsForBasicStat(t: ColumnStatistics, o: ColumnStatistics): Boolean = {
      // skip check the bytesOnDisk
      t.getNumberOfValues == o.getNumberOfValues && t.hasNull == o.hasNull
    }

    // Note: no corresponding hashCode, should not be used in Map key
    override def equals(obj: Any): Boolean = obj match {
      case _@ColumnStat(thatCs) =>
        if (!equalsForBasicStat(cs, thatCs)) return false
        (cs, thatCs) match {
          case (doubleStat: DoubleColumnStatistics, otherDoubleStat: DoubleColumnStatistics) =>
            Objects.equals(doubleStat.getMinimum, otherDoubleStat.getMinimum) &&
                Objects.equals(doubleStat.getMaximum, otherDoubleStat.getMaximum) &&
                Objects.equals(doubleStat.getSum, otherDoubleStat.getSum)
          case (decimalStat: DecimalColumnStatistics, otherDecimalStat: DecimalColumnStatistics) =>
            Objects.equals(decimalStat.getMinimum, otherDecimalStat.getMinimum) &&
                Objects.equals(decimalStat.getMaximum, otherDecimalStat.getMaximum) &&
                Objects.equals(decimalStat.getSum, otherDecimalStat.getSum)
          case (boolStat: BooleanColumnStatistics, otherBoolStat: BooleanColumnStatistics) =>
            Objects.equals(boolStat.getFalseCount, otherBoolStat.getFalseCount)
          case (integerStat: IntegerColumnStatistics, otherIntegerStat: IntegerColumnStatistics) =>
            Objects.equals(integerStat.getMinimum, otherIntegerStat.getMinimum) &&
                Objects.equals(integerStat.getMaximum, otherIntegerStat.getMaximum) &&
                Objects.equals(integerStat.isSumDefined, otherIntegerStat.isSumDefined) &&
                Objects.equals(integerStat.getSum, otherIntegerStat.getSum)
          case (binaryStat: BinaryColumnStatistics, otherBinaryStat: BinaryColumnStatistics) =>
            Objects.equals(binaryStat.getSum, otherBinaryStat.getSum)
          case (tsStat: TimestampColumnStatistics, otherTsStat: TimestampColumnStatistics) =>
            Objects.equals(tsStat.getMinimum, otherTsStat.getMinimum) &&
                Objects.equals(tsStat.getMaximum, otherTsStat.getMaximum) &&
                Objects.equals(tsStat.getMinimumUTC, otherTsStat.getMinimumUTC) &&
                Objects.equals(tsStat.getMaximumUTC, otherTsStat.getMaximumUTC)
          case (stat: ColumnStatistics, otherStat: ColumnStatistics) if OrcStatisticShim
              .supports(stat, otherStat) => OrcStatisticShim.equals(stat, otherStat)
          case (t: ColumnStatisticsImpl, o: ColumnStatisticsImpl) => equalsForBasicStat(t, o)
          case (t@_, _@_) => throw new IllegalStateException("Unexpected type: " + t.getClass)
        }
      case _ => false
    }
  }

  case class StripeFooterStat(streams: Seq[StreamStat], encodings: Seq[String])

  case class StreamStat(columnIndex: Int, streamKind: String)

  val statTestSchema =
    """
    struct<
      c01: boolean,
      c02: byte,
      c03: short,
      c04: int,
      c05: long,
      c06: decimal,
      c07: float,
      c08: double,
      c09: string,
      c10: timestamp,
      c11: date,
      c12: struct<
        c12_01: timestamp,
        c12_02: decimal,
        c12_03: double
      >,
      c13: array<
        decimal
      >,
      c14: map<
        byte,
        float
      >,
      c15: array<
        struct<
          c15_01: date,
          c15_02: array<short>
        >
      >
    >
    """
}
