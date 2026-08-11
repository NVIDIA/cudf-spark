/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.sql.catalyst.json.rapids.JsonPartitionReader
import org.apache.spark.sql.rapids.GpuJsonReadCommon
import org.apache.spark.sql.types._

class JsonScanRetrySuite extends RmmSparkRetrySuiteBase {
  test("test simple retry") {
    val bufferer = FilterEmptyHostLineBuffererFactory.createBufferer(100, Array('\n'.toByte))
    bufferer.add("{\"a\": 1, \"b\": 2".getBytes, 0, 14)

    val cudfSchema = GpuColumnVector.from(StructType(Seq(StructField("a", IntegerType),
      StructField("b", IntegerType))))
    val opts = GpuJsonReadCommon.baseCudfJsonOptionsBuilder().withLines(true).build()
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    val table = JsonPartitionReader.readToTable(bufferer, cudfSchema, NoopMetric,
      opts, "JSON", null)
    table.close()
    // We don't have any good way to verify that the retry was thrown, but we are going to trust
    // that it was.
  }
}
