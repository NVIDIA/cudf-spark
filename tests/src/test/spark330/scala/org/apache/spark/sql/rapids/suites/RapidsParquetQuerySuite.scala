/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.GpuFilterExec

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.sql.execution.datasources.parquet.ParquetQuerySuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait

class RapidsParquetQuerySuite extends ParquetQuerySuite with RapidsSQLTestsBaseTrait {
  import testImplicits._

  testRapids("SPARK-26677: negated null-safe equality comparison should not filter " +
    "matched row groups") {
    withAllParquetReaders {
      withTempPath { path =>
        // Repeated values for dictionary encoding.
        Seq(Some("A"), Some("A"), None).toDF.repartition(1)
          .write.parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        checkAnswer(stripSparkFilterRapids(df.where("NOT (value <=> 'A')")), df)
      }
    }
  }

  testRapids("SPARK-34212 Parquet should read decimals correctly") {
    def readParquet(schema: String, path: String): DataFrame = {
      spark.read.schema(schema).parquet(path)
    }

    def hasSchemaConvertError(e: SparkException): Boolean = {
      Iterator
        .iterate(e: Throwable)(_.getCause)
        .takeWhile(_ != null)
        .exists(_.isInstanceOf[SchemaColumnConvertNotSupportedException])
    }

    def readWithVectorized(
        schema: String,
        path: String,
        vectorized: Boolean): Either[SparkException, Seq[Row]] = {
      try {
        var rows: Seq[Row] = Nil
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorized.toString) {
          rows = readParquet(schema, path).collect().toSeq
        }
        Right(rows)
      } catch {
        case e: SparkException => Left(e)
      }
    }

    def assertVectorizedEquivalentOrUnsupported(schema: String, path: String): Unit = {
      val nonVectorized = readWithVectorized(schema, path, vectorized = false)
      val vectorized = readWithVectorized(schema, path, vectorized = true)
      (nonVectorized, vectorized) match {
        case (Right(expected), Right(actual)) =>
          assert(actual == expected, s"Unexpected vectorized result for schema: $schema")
        case (Right(_), Left(e)) =>
          assert(hasSchemaConvertError(e), s"Unexpected vectorized failure for schema: $schema")
        case (Left(nonVecErr), Left(vecErr)) =>
          assert(nonVecErr.getMessage.contains("FAILED_READ_FILE") ||
            hasSchemaConvertError(nonVecErr),
            s"Unexpected non-vectorized failure for schema: $schema")
          assert(vecErr.getMessage.contains("FAILED_READ_FILE") || hasSchemaConvertError(vecErr),
            s"Unexpected vectorized failure type for schema: $schema")
        case (Left(nonVecErr), Right(_)) =>
          fail(s"Vectorized read succeeded while non-vectorized failed for schema: $schema: " +
            nonVecErr.getMessage)
      }
    }

    withAllParquetReaders {
      withTempPath { path =>
        val filePath = path.toString
        val df = sql(
          "SELECT 1.0 a, CAST(1.23 AS DECIMAL(17, 2)) b, CAST(1.23 AS DECIMAL(36, 2)) c")
        df.write.parquet(filePath)

        // We can read the decimal parquet field with a larger precision, if scale is the same.
        val schema1 = "a DECIMAL(9, 1), b DECIMAL(18, 2), c DECIMAL(38, 2)"
        checkAnswer(readParquet(schema1, filePath), df)
        val schema2 = "a DECIMAL(18, 1), b DECIMAL(38, 2), c DECIMAL(38, 2)"
        checkAnswer(readParquet(schema2, filePath), df)
      }

      withTempPath { path =>
        val filePath = path.toString
        val df = sql(
          "SELECT 1.0 a, CAST(1.23 AS DECIMAL(17, 2)) b, CAST(1.23 AS DECIMAL(36, 2)) c")
        df.write.parquet(filePath)

        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
          val schema1 = "a DECIMAL(3, 2), b DECIMAL(18, 3), c DECIMAL(37, 3)"
          checkAnswer(readParquet(schema1, filePath), df)
          val schema2 = "a DECIMAL(3, 0), b DECIMAL(18, 1), c DECIMAL(37, 1)"
          checkAnswer(readParquet(schema2, filePath), Row(1, 1.2, 1.2))
        }

        val schema1 = "a DECIMAL(3, 2), b DECIMAL(18, 3), c DECIMAL(37, 3)"
        assertVectorizedEquivalentOrUnsupported(schema1, filePath)
        Seq("a DECIMAL(3, 0)", "b DECIMAL(18, 1)", "c DECIMAL(37, 1)")
          .foreach(schema => assertVectorizedEquivalentOrUnsupported(schema, filePath))
      }
    }

    // tests for parquet types without decimal metadata.
    withAllParquetReaders {
      withTempPath { path =>
        val filePath = path.toString
        val df = sql(s"SELECT 1 a, 123456 b, " +
          s"${Int.MaxValue.toLong * 10} c, CAST('1.2' AS BINARY) d")
        df.write.parquet(filePath)

        Seq(
          "a DECIMAL(3, 2)",
          "a DECIMAL(11, 2)",
          "b DECIMAL(3, 2)",
          "b DECIMAL(11, 1)",
          "c DECIMAL(11, 1)",
          "c DECIMAL(13, 0)",
          "c DECIMAL(22, 0)",
          "c DECIMAL(18, 1)",
          "d DECIMAL(3, 2)",
          "d DECIMAL(37, 1)")
          .foreach(schema => assertVectorizedEquivalentOrUnsupported(schema, filePath))
      }
    }
  }

  def stripSparkFilterRapids(df: DataFrame): DataFrame = {
    val schema = df.schema
    val withoutFilters = df.queryExecution.executedPlan.transform {
      case GpuFilterExec(_, child) => child
    }
    spark.internalCreateDataFrame(withoutFilters.execute(), schema)
  }
}
