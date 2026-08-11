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

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, NoopCache}
import org.apache.spark.sql.execution.datasources.json.JsonSuite
import org.apache.spark.sql.execution.datasources.v2.json.JsonScanBuilder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.{RapidsJsonConfTrait, RapidsSQLTestsBaseTrait}
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class RapidsJsonSuite
  extends JsonSuite with RapidsSQLTestsBaseTrait with RapidsJsonConfTrait {
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
