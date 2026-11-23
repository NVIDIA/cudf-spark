/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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
{"spark":["330"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.{Date, Timestamp}
import java.time.Instant

import com.nvidia.spark.rapids.GpuProjectExec

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, NoopCache}
import org.apache.spark.sql.execution.datasources.json.{JsonFileFormat, JsonSuite}
import org.apache.spark.sql.execution.datasources.v2.json.JsonScanBuilder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.{RapidsJsonConfTrait, RapidsSQLTestsBaseTrait}
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class RapidsJsonSuite
  extends JsonSuite with RapidsSQLTestsBaseTrait with RapidsJsonConfTrait {

  testRapids("SPARK-30960, SPARK-31641: LEGACY JSON datetime scan falls back to CPU") {
    // Use the lenient date from SPARK-30960 and the policy-sensitive timestamp offset
    // from JsonSuite's "exception mode for parsing date/timestamp string" test.
    val input = "{'t': '2020-01-27T20:06:11.847-08000', 'd': '2020-1-12 T'}"
    val schema = StructType.fromDDL("t timestamp, d date")
    withSQLConf(
      SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
      SQLConf.USE_V1_SOURCE_LIST.key -> "json",
      "spark.rapids.sql.json.read.datetime.enabled" -> "false") {
      withTempPath { path =>
        Files.write(path.toPath, input.getBytes(StandardCharsets.UTF_8))
        def query = spark.read.schema(schema)
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSz")
          .json(path.getCanonicalPath)
          .selectExpr("t", "d", "date_add(d, 1) AS next_day")
        withSQLConf("spark.rapids.sql.enabled" -> "false") {
          val cpu = query
          val expected = cpu.collect().toSeq
          assert(expected == Seq(Row(Timestamp.from(Instant.parse("2020-01-28T04:06:11.847Z")),
            Date.valueOf("2020-01-12"), Date.valueOf("2020-01-13"))))
          withSQLConf("spark.rapids.sql.enabled" -> "true") {
            val gpu = query
            checkAnswer(gpu, expected)
            assert(gpu.schema == cpu.schema)
            val projects = getExecutedPlan(gpu).collect { case project: GpuProjectExec => project }
            assert(projects.exists(_.exists {
              case scan: FileSourceScanExec =>
                scan.relation.fileFormat.isInstanceOf[JsonFileFormat] &&
                  scan.requiredSchema == schema
              case _ => false
            }), s"Expected a GPU project above a CPU JSON datetime scan: " +
              s"${gpu.queryExecution.executedPlan}")
          }
        }
        withSQLConf(
          "spark.rapids.sql.enabled" -> "false",
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "CORRECTED") {
          checkAnswer(query.select("t"), Row(null))
        }
      }
    }
  }
}

class RapidsJsonV1Suite extends RapidsJsonSuite with RapidsSQLTestsBaseTrait {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "json")
}

class RapidsJsonV2Suite extends RapidsJsonSuite with RapidsSQLTestsBaseTrait {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("get pushed filters") {
    val attr = "col"
    def getBuilder(path: String): JsonScanBuilder = {
      val fileIndex = new InMemoryFileIndex(
        spark,
        Seq(new org.apache.hadoop.fs.Path(path, "file.json")),
        Map.empty,
        None,
        NoopCache)
      val schema = new StructType().add(attr, IntegerType)
      val options = CaseInsensitiveStringMap.empty()
      new JsonScanBuilder(spark, fileIndex, schema, schema, options)
    }
    val filters: Array[sources.Filter] = Array(sources.IsNotNull(attr))
    withSQLConf(SQLConf.JSON_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath {
        file =>
          val scanBuilder = getBuilder(file.getCanonicalPath)
          assert(scanBuilder.pushDataFilters(filters) === filters)
      }
    }

    withSQLConf(SQLConf.JSON_FILTER_PUSHDOWN_ENABLED.key -> "false") {
      withTempPath {
        file =>
          val scanBuilder = getBuilder(file.getCanonicalPath)
          assert(scanBuilder.pushDataFilters(filters) === Array.empty[sources.Filter])
      }
    }
  }
}

class RapidsJsonLegacyTimeParserSuite extends RapidsJsonSuite with RapidsSQLTestsBaseTrait {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.LEGACY_TIME_PARSER_POLICY, "legacy")
}
