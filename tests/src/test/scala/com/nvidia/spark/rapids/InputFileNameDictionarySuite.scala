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

import java.io.File

import ai.rapids.cudf.{ColumnVector, DType}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.functions.{col, input_file_name, substring_index}
import org.apache.spark.sql.rapids.{ExecutionPlanCaptureCallback, GpuInputFileName}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.vectorized.ColumnarBatch

class InputFileNameDictionarySuite extends SparkQueryCompareTestSuite {

  private val conf = new SparkConf().set("spark.sql.adaptive.enabled", "false")

  private def writeInput(path: File): Unit = withCpuSparkSession { spark =>
    spark.range(0, 30, 1, 3)
      .selectExpr("id AS a", "id * 2 AS b", "CAST(id AS STRING) AS c", "id % 2 AS d",
        "id * 3 AS e")
      .write.parquet(path.getCanonicalPath)
  }

  /** The outputDictionary flag of every GpuInputFileName in an executed plan. */
  private def encodings(plan: SparkPlan): Seq[Boolean] =
    plan.flatMap(_.expressions.flatMap(_.collect {
      case f: GpuInputFileName => f.outputDictionary
    }))

  /**
   * Runs `query` on the CPU and on the GPU, checks the GPU plan's input_file_name encodings and
   * that both return the same rows.
   */
  private def check(expectedEncodings: Seq[Boolean])(query: DataFrame => DataFrame): Unit = {
    withTempPath { path =>
      writeInput(path)
      def run(spark: SparkSession): (Seq[Boolean], Seq[String]) = {
        val df = query(spark.read.parquet(path.getCanonicalPath))
        (encodings(df.queryExecution.executedPlan), df.collect().map(_.toString).sorted.toSeq)
      }
      val (_, expected) = withCpuSparkSession(run, conf)
      val (gpuEncodings, actual) = withGpuSparkSession(run, conf)
      assert(gpuEncodings == expectedEncodings)
      assert(actual == expected)
    }
  }

  /**
   * Writes `select a, c, input_file_name() AS f` on the CPU and on the GPU, checks the GPU write
   * plan's input_file_name encodings and that both wrote the same rows.
   */
  private def checkWrite(expectedEncodings: Seq[Boolean], format: String,
      partitionBy: Seq[String] = Seq.empty, writeConf: SparkConf = conf): Unit = {
    withTempPath { input =>
      writeInput(input)
      def write(output: File)(spark: SparkSession): Unit =
        spark.read.parquet(input.getCanonicalPath)
          .select(col("a"), col("c"), input_file_name().as("f"))
          .write.partitionBy(partitionBy: _*).format(format).save(output.getCanonicalPath)
      withTempPath { cpuOutput =>
        withTempPath { gpuOutput =>
          withCpuSparkSession(write(cpuOutput), writeConf)
          val gpuEncodings = withGpuSparkSession({ spark =>
            ExecutionPlanCaptureCallback.startCapture()
            write(gpuOutput)(spark)
            ExecutionPlanCaptureCallback.getResultsWithTimeout()
              .flatMap(p => encodings(ExecutionPlanCaptureCallback.extractExecutedPlan(p)))
              .toSeq
          }, writeConf)
          def read(output: File): Seq[String] = withCpuSparkSession { spark =>
            spark.read.format(format).load(output.getCanonicalPath)
              .collect().map(_.toString).sorted.toSeq
          }
          assert(gpuEncodings == expectedEncodings)
          assert(read(gpuOutput) == read(cpuOutput))
        }
      }
    }
  }

  // At most 4 columns use ColumnarToRowIterator, more use the accelerated RowConversion path.
  Seq(Seq("a"), Seq("a", "b", "c"), Seq("a", "b", "c", "d")).foreach { cols =>
    test(s"dictionary for input_file_name() with ${cols.size + 1} columns read as rows") {
      check(Seq(true)) { df =>
        df.select(cols.map(col) :+ input_file_name().as("f"): _*)
      }
    }
  }

  test("dictionary through pass-through projects and filters not using it") {
    check(Seq(true)) { df =>
      df.select(col("a"), col("c"), input_file_name().as("f"))
        .where(col("a") > 3)
        .select(col("f").as("g"), col("c"))
    }
  }

  test("string when a filter uses input_file_name()") {
    check(Seq(false)) { df =>
      df.select(col("a"), input_file_name().as("f")).where(col("f").endsWith(".parquet"))
    }
  }

  test("string when an expression reads input_file_name()") {
    check(Seq(false)) { df =>
      df.select(col("a"), substring_index(input_file_name(), "/", -1).as("f"))
    }
  }

  test("string when a project passes the column through and also reads it") {
    check(Seq(false)) { df =>
      df.select(col("a"), input_file_name().as("f"))
        .select(col("a"), col("f"), substring_index(col("f"), "/", -1).as("name"))
    }
  }

  test("string when input_file_name() is aggregated") {
    check(Seq(false)) { df =>
      df.select(col("a"), input_file_name().as("f")).groupBy("f").count()
    }
  }

  Seq("parquet", "orc").foreach { format =>
    test(s"dictionary for input_file_name() written to $format") {
      checkWrite(Seq(true), format)
    }
  }

  test("dictionary for input_file_name() written without planned writes") {
    checkWrite(Seq(true), "parquet",
      writeConf = conf.clone().set("spark.sql.optimizer.plannedWrite.enabled", "false"))
  }

  test("string when the write is partitioned") {
    checkWrite(Seq(false), "parquet", partitionBy = Seq("a"))
  }

  test("host reads of string dictionaries with every index width") {
    Seq(DType.INT8, DType.INT16, DType.INT32, DType.INT64).foreach { indexType =>
      val dictionary = withResource(ColumnVector.fromStrings("a", "bb")) { keys =>
        withResource(ColumnVector.fromInts(1, 0, 1)) { ints =>
          withResource(ints.castTo(indexType)) { indices =>
            ColumnVector.makeDictionary(keys, indices)
          }
        }
      }
      withResource(GpuColumnVector.from(dictionary, StringType)) { gpu =>
        withResource(gpu.copyToHost()) { host =>
          assert((0 until 3).map(host.getUTF8String(_).toString) == Seq("bb", "a", "bb"))
          assert((0 until 3).map(i => new String(host.getBinary(i), "UTF-8")) ==
            Seq("bb", "a", "bb"))
        }
      }
    }
  }

  test("dictionary output has one key and every row points at it, including 0 rows") {
    Seq(0, 3).foreach { numRows =>
      // No file is being read here, so the path is "".
      withResource(GpuInputFileName(outputDictionary = true)
          .columnarEval(new ColumnarBatch(Array.empty, numRows))) { result =>
        val dictionary = result.getBase
        assert(dictionary.getType == DType.DICTIONARY32)
        assert(dictionary.getRowCount == numRows)
        withResource(dictionary.getDictionaryKeys) { keys =>
          assert(keys.getRowCount == 1)
        }
        withResource(dictionary.dictionaryDecode()) { decoded =>
          withResource(decoded.copyToHost()) { host =>
            assert((0 until numRows).map(host.getJavaString(_)) == Seq.fill(numRows)(""))
          }
        }
      }
    }
  }
}
