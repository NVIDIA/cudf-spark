/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._

class BloomFilterAggregateQuerySuite extends BloomFilterAggregateQuerySuiteBase {

  // test with GPU bloom build, GPU bloom probe
  for (numEstimated <- Seq(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS.defaultValue.get)) {
    for (numBits <- Seq(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS.defaultValue.get)) {
      testSparkResultsAreEqual(
        s"might_contain GPU build GPU probe estimated=$numEstimated numBits=$numBits",
        buildData,
        conf = bloomFilterEnabledConf.clone()
      )(doBloomFilterTest(numEstimated, numBits))
    }
  }

  // test with CPU bloom build, GPU bloom probe
  for (numEstimated <- Seq(4096L, 4194304L, Long.MaxValue,
    SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS.defaultValue.get)) {
    for (numBits <- Seq(4096L, 4194304L,
      SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS.defaultValue.get)) {
      ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
        s"might_contain CPU build GPU probe estimated=$numEstimated numBits=$numBits",
        buildData,
        Seq("ObjectHashAggregateExec", "ShuffleExchangeExec"),
        conf = bloomFilterEnabledConf.clone()
          .set("spark.rapids.sql.expression.BloomFilterAggregate", "false")
      )(doBloomFilterTest(numEstimated, numBits))(getPlanValidator("ObjectHashAggregateExec"))
    }
  }

  // test with GPU bloom build, CPU bloom probe - bridge disabled
  for (numEstimated <- Seq(4096L, 4194304L, Long.MaxValue,
    SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS.defaultValue.get)) {
    for (numBits <- Seq(4096L, 4194304L,
      SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS.defaultValue.get)) {
      ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
        s"might_contain GPU build CPU probe (bridge disabled) " +
          s"estimated=$numEstimated numBits=$numBits",
        buildData,
        Seq("LocalTableScanExec", "ProjectExec", "ShuffleExchangeExec"),
        conf = bloomFilterEnabledConf.clone()
          .set("spark.rapids.sql.expression.BloomFilterMightContain", "false")
          .set("spark.rapids.sql.expression.cpuBridge.enabled", "false")
      )(doBloomFilterTest(numEstimated, numBits))(getPlanValidator("ProjectExec"))
    }
  }

  // test with GPU bloom build, CPU bloom probe - bridge enabled
  for (numEstimated <- Seq(4096L, 4194304L, Long.MaxValue,
    SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS.defaultValue.get)) {
    for (numBits <- Seq(4096L, 4194304L,
      SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS.defaultValue.get)) {
      ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
        s"might_contain GPU build CPU probe (bridge enabled) " +
          s"estimated=$numEstimated numBits=$numBits",
        buildData,
        Seq("LocalTableScanExec", "ShuffleExchangeExec", 
          "BloomFilterMightContain", "ScalarSubquery"),
        conf = bloomFilterEnabledConf.clone()
          .set("spark.rapids.sql.expression.BloomFilterMightContain", "false")
          .set("spark.rapids.sql.expression.cpuBridge.enabled", "true")
      )(doBloomFilterTest(numEstimated, numBits)) { (cpuPlan, gpuPlan) =>
        // With bridge enabled, expect GpuProjectExec instead of ProjectExec
        ExecutionPlanCaptureCallback.assertContains(gpuPlan, "GpuProjectExec")
        // Validate that BloomFilterMightContain fell back via bridge
        ExecutionPlanCaptureCallback.assertDidFallBack(gpuPlan, "BloomFilterMightContain")
      }
    }
  }

  // test with partial/final-only GPU bloom build, CPU bloom probe - bridge disabled
  for (mode <- Seq("partial", "final")) {
    for (numEstimated <- Seq(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_ITEMS.defaultValue.get)) {
      for (numBits <- Seq(SQLConf.RUNTIME_BLOOM_FILTER_MAX_NUM_BITS.defaultValue.get)) {
        ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
          s"might_contain GPU $mode build CPU probe (bridge disabled) " +
            s"estimated=$numEstimated numBits=$numBits",
          buildData,
          Seq("ObjectHashAggregateExec", "ProjectExec", "ShuffleExchangeExec"),
          conf = bloomFilterEnabledConf.clone()
            .set("spark.rapids.sql.expression.BloomFilterMightContain", "false")
            .set("spark.rapids.sql.hashAgg.replaceMode", mode)
            .set("spark.rapids.sql.expression.cpuBridge.enabled", "false")
        )(doBloomFilterTest(numEstimated, numBits))(getPlanValidator("ObjectHashAggregateExec"))
      }
    }
  }

  // V1 literal: version=1, numHashes=5, numLongs=3, followed by 3 longs of bit data
  testSparkResultsAreEqual(
    "might_contain with V1 literal bloom filter buffer",
    spark => spark.range(1, 1).asInstanceOf[DataFrame],
    conf=bloomFilterEnabledConf.clone()) {
    df =>
      withExposedSqlFuncs(df.sparkSession) { spark =>
        spark.sql(
          """SELECT might_contain(
            |X'00000001000000050000000343A2EC6EA8C117E2D3CDB767296B144FC5BFBCED9737F267',
            |cast(201 as long))""".stripMargin)
      }
  }

  testSparkResultsAreEqual(
    "might_contain with all NULL inputs",
    spark => spark.range(1, 1).asInstanceOf[DataFrame],
    conf=bloomFilterEnabledConf.clone()) {
    df =>
      withExposedSqlFuncs(df.sparkSession) { spark =>
        spark.sql(
          """
            |SELECT might_contain(null, null) both_null,
            |       might_contain(null, 1L) null_bf,
            |       might_contain((SELECT bloom_filter_agg(cast(id as long)) from range(1, 10000)),
            |            null) null_value
          """.stripMargin)
      }
  }

  testSparkResultsAreEqual(
    "bloom_filter_agg with empty input",
    spark => spark.range(1, 1).asInstanceOf[DataFrame],
    conf=bloomFilterEnabledConf.clone()) {
    df =>
      withExposedSqlFuncs(df.sparkSession) { spark =>
        spark.sql("""SELECT bloom_filter_agg(cast(id as long)) from range(1, 1)""")
      }
  }
}
