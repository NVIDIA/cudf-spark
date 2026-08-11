/*
 * Copyright (c) 2019, 2021, NVIDIA CORPORATION.
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

import org.apache.spark.SparkConf

class SortExecSuite extends SparkQueryCompareTestSuite {
  // force a sortMergeJoin
  private val sortJoinConf = new SparkConf().set("spark.sql.autoBroadcastJoinThreshold", "-1").
    set("spark.sql.join.preferSortMergeJoin", "true").set("spark.sql.exchange.reuse", "false")

  testSparkResultsAreEqual2("join longs", longsDf, longsDf, conf = sortJoinConf,
    sort = true) {
    (dfA, dfB) => dfA.join(dfB, dfA("longs") === dfB("longs"))
  }

  private val sortJoinMultiBatchConf = sortJoinConf.set(RapidsConf.GPU_BATCH_SIZE_BYTES.key, "3")

  testSparkResultsAreEqual2("join longs multiple batches", longsDf, longsDf,
      conf = sortJoinMultiBatchConf, sort = true) {
    (dfA, dfB) => dfA.join(dfB, dfA("longs") === dfB("longs"))
  }

  testSparkResultsAreEqual("GpuRangePartitioning with numparts > numvalues v1", longsCsvDf,
    conf = makeBatchedBytes(1, enableCsvConf())) {
    df => df.filter(df.col("longs").gt(1)).sort(df.col("longs"))
  }

  testSparkResultsAreEqual("GpuRangePartitioning with numparts > numvalues v2", longsCsvDf,
    conf = enableCsvConf().set("spark.sql.shuffle.partitions", "4"), repart = 4) {
    df => df.filter(df.col("longs").lt(-800)).sort(df.col("longs"))
  }
}
