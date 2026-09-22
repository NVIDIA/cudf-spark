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
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import java.util.TimeZone

import org.apache.spark.sql.{JsonFunctionsSuite, Row}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.rapids.utils.{RapidsJsonConfTrait, RapidsSQLTestsTrait}
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}

class RapidsJsonFunctionsSuite
    extends JsonFunctionsSuite with RapidsSQLTestsTrait with RapidsJsonConfTrait {

  import testImplicits._

  val originalTimeZone = TimeZone.getDefault
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Set timezone to UTC to avoid fallback, so that tests run on GPU to detect bugs
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  }

  override def afterAll(): Unit = {
    TimeZone.setDefault(originalTimeZone)
    super.afterAll()
  }

  testRapids("SPARK-33134: return partial results for root JSON objects on GPU") {
    val st = new StructType()
      .add("c1", LongType)
      .add("c2", ArrayType(new StructType().add("c3", LongType).add("c4", StringType)))

    val df1 = Seq("""{"c2": [19], "c1": 123456}""").toDF("c0")
    checkAnswer(df1.select(from_json($"c0", st)), Row(Row(123456, null)))

    val df2 = Seq("""{"data": {"c2": [19], "c1": 123456}}""").toDF("c0")
    checkAnswer(
      df2.select(from_json($"c0", new StructType().add("data", st))),
      Row(Row(null)))
  }
}
