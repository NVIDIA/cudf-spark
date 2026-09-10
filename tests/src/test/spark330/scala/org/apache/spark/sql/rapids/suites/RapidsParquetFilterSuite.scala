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
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import com.nvidia.spark.rapids.parquet.GpuParquetScan
import com.nvidia.spark.rapids.shims.GpuBatchScanExec

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.execution.datasources.parquet.{
  ParquetFilterSuite, ParquetV1FilterSuite, ParquetV2FilterSuite}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType.{
  TIMESTAMP_MICROS, TIMESTAMP_MILLIS}
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.utils.{RapidsParquetResourceTest, RapidsSQLTestsBaseTrait}

trait RapidsParquetFilterCorrectnessTests extends RapidsSQLTestsBaseTrait {
  this: ParquetFilterSuite =>

  protected def assertGpuParquetScan(df: DataFrame): Unit = {
    val executed = getExecutedPlan(df)
    assert(executed.exists {
      case _: GpuFileSourceScanExec => true
      case scan: GpuBatchScanExec => scan.scan.isInstanceOf[GpuParquetScan]
      case _ => false
    }, s"Expected a GPU Parquet scan in plan:\n${df.queryExecution.executedPlan}")
  }

  protected def checkGpuParquetAnswer(df: DataFrame, expected: Seq[Row]): Unit = {
    assertGpuParquetScan(df)
    checkAnswer(df, expected)
  }

  private def withWrittenParquet(df: DataFrame)(f: DataFrame => Unit): Unit = {
    withTempPath { path =>
      df.write.parquet(path.getAbsolutePath)
      f(spark.read.parquet(path.getAbsolutePath))
    }
  }

  private def withParquetFilterPushdown(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED.key -> "true") {
      f
    }
  }

  testRapids("filter pushdown - binary final output correctness") {
    import testImplicits._

    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes(StandardCharsets.UTF_8)
    }

    withParquetFilterPushdown {
      withWrittenParquet((1 to 4).map(i => (i, Option(i.b))).toDF("id", "b")) { df =>
        def idsFor(predicate: Column): DataFrame = df.where(predicate).select("id")

        checkGpuParquetAnswer(idsFor(col("b") === lit(1.b)), Seq(Row(1)))
        checkGpuParquetAnswer(idsFor(col("b") <=> lit(1.b)), Seq(Row(1)))
        checkGpuParquetAnswer(idsFor(col("b").isNull), Seq.empty)
        checkGpuParquetAnswer(idsFor(col("b").isNotNull), (1 to 4).map(Row(_)))
        checkGpuParquetAnswer(idsFor(col("b") =!= lit(1.b)), (2 to 4).map(Row(_)))
        checkGpuParquetAnswer(idsFor(col("b") < lit(2.b)), Seq(Row(1)))
        checkGpuParquetAnswer(idsFor(col("b") > lit(3.b)), Seq(Row(4)))
        checkGpuParquetAnswer(idsFor(col("b") <= lit(1.b)), Seq(Row(1)))
        checkGpuParquetAnswer(idsFor(col("b") >= lit(4.b)), Seq(Row(4)))
        checkGpuParquetAnswer(idsFor(!(col("b") < lit(4.b))), Seq(Row(4)))
        checkGpuParquetAnswer(
          idsFor(col("b") < lit(2.b) || col("b") > lit(3.b)),
          Seq(Row(1), Row(4)))
      }
    }
  }

  testRapids("filter pushdown - date final output correctness") {
    import testImplicits._

    withParquetFilterPushdown {
      Seq(false, true).foreach { java8Api =>
        withSQLConf(
          SQLConf.DATETIME_JAVA8API_ENABLED.key -> java8Api.toString,
          SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "CORRECTED") {
          val data = Seq(
            (1, Option(Date.valueOf("1000-01-01"))),
            (2, Option(Date.valueOf("2018-03-19"))),
            (3, Option(Date.valueOf("2018-03-20"))),
            (4, Option(Date.valueOf("2018-03-21"))))
          withWrittenParquet(data.toDF("id", "d")) { df =>
            def idsWhere(predicate: String): DataFrame = df.where(predicate).select("id")

            checkGpuParquetAnswer(idsWhere("d is null"), Seq.empty)
            checkGpuParquetAnswer(idsWhere("d is not null"), (1 to 4).map(Row(_)))
            checkGpuParquetAnswer(idsWhere("d = date '1000-01-01'"), Seq(Row(1)))
            checkGpuParquetAnswer(idsWhere("d <=> date '1000-01-01'"), Seq(Row(1)))
            checkGpuParquetAnswer(
              idsWhere("d != date '1000-01-01'"),
              Seq(Row(2), Row(3), Row(4)))
            checkGpuParquetAnswer(idsWhere("d < date '2018-03-19'"), Seq(Row(1)))
            checkGpuParquetAnswer(idsWhere("d > date '2018-03-20'"), Seq(Row(4)))
            checkGpuParquetAnswer(idsWhere("d <= date '1000-01-01'"), Seq(Row(1)))
            checkGpuParquetAnswer(idsWhere("d >= date '2018-03-21'"), Seq(Row(4)))
            checkGpuParquetAnswer(idsWhere("not (d < date '2018-03-21')"), Seq(Row(4)))
            checkGpuParquetAnswer(
              idsWhere("d < date '2018-03-19' or d > date '2018-03-20'"),
              Seq(Row(1), Row(4)))
            checkGpuParquetAnswer(
              idsWhere("d in (date '2018-03-19', date '2018-03-20', " +
                "date '2018-03-21', date '2018-03-22')"),
              Seq(Row(2), Row(3), Row(4)))
          }
        }
      }
    }
  }

  testRapids("filter pushdown - timestamp final output correctness") {
    import testImplicits._

    withParquetFilterPushdown {
      Seq(false, true).foreach { java8Api =>
        Seq(TIMESTAMP_MILLIS, TIMESTAMP_MICROS).foreach { timestampType =>
          withSQLConf(
            SQLConf.DATETIME_JAVA8API_ENABLED.key -> java8Api.toString,
            SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "CORRECTED",
            SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> timestampType.toString) {
            val timestamps = if (timestampType == TIMESTAMP_MILLIS) {
              Seq(
                "1000-06-14 08:28:53.123",
                "1582-06-15 08:28:53.001",
                "1900-06-16 08:28:53.0",
                "2018-06-17 08:28:53.999")
            } else {
              Seq(
                "1000-06-14 08:28:53.123456",
                "1582-06-15 08:28:53.123456",
                "1900-06-16 08:28:53.123456",
                "2018-06-17 08:28:53.123456")
            }
            val data = timestamps.zipWithIndex.map { case (ts, idx) =>
              (idx + 1, Option(Timestamp.valueOf(ts)))
            }
            withWrittenParquet(data.toDF("id", "ts")) { df =>
              def idsWhere(predicate: String): DataFrame = df.where(predicate).select("id")
              def tsLiteral(index: Int): String = s"timestamp '${timestamps(index)}'"

              checkGpuParquetAnswer(idsWhere("ts is null"), Seq.empty)
              checkGpuParquetAnswer(idsWhere("ts is not null"), (1 to 4).map(Row(_)))
              checkGpuParquetAnswer(idsWhere(s"ts = ${tsLiteral(0)}"), Seq(Row(1)))
              checkGpuParquetAnswer(idsWhere(s"ts <=> ${tsLiteral(0)}"), Seq(Row(1)))
              checkGpuParquetAnswer(
                idsWhere(s"ts != ${tsLiteral(0)}"),
                Seq(Row(2), Row(3), Row(4)))
              checkGpuParquetAnswer(idsWhere(s"ts < ${tsLiteral(1)}"), Seq(Row(1)))
              checkGpuParquetAnswer(idsWhere(s"ts > ${tsLiteral(2)}"), Seq(Row(4)))
              checkGpuParquetAnswer(idsWhere(s"ts <= ${tsLiteral(0)}"), Seq(Row(1)))
              checkGpuParquetAnswer(idsWhere(s"ts >= ${tsLiteral(3)}"), Seq(Row(4)))
              checkGpuParquetAnswer(idsWhere(s"not (ts < ${tsLiteral(3)})"), Seq(Row(4)))
              checkGpuParquetAnswer(
                idsWhere(s"ts < ${tsLiteral(1)} or ts > ${tsLiteral(2)}"),
                Seq(Row(1), Row(4)))
              checkGpuParquetAnswer(
                idsWhere(s"ts in (${tsLiteral(1)}, ${tsLiteral(2)}, ${tsLiteral(3)})"),
                Seq(Row(2), Row(3), Row(4)))
            }
          }
        }
      }
    }
  }
}

class RapidsParquetV1FilterSuite
  extends ParquetV1FilterSuite
  with RapidsSQLTestsBaseTrait
  with RapidsParquetResourceTest
  with RapidsParquetFilterCorrectnessTests

class RapidsParquetV2FilterSuite
  extends ParquetV2FilterSuite
  with RapidsSQLTestsBaseTrait
  with RapidsParquetResourceTest
  with RapidsParquetFilterCorrectnessTests
