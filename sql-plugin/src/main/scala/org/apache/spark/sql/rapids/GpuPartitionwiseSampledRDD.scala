/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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
package org.apache.spark.sql.rapids

import org.apache.spark.rdd.{PartitionwiseSampledRDD, RDD}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils
import org.apache.spark.util.random.RandomSampler

// PartitionwiseSampledRDD is private in [spark] package, so this is a forward to access it
class GpuPartitionwiseSampledRDD(prev: RDD[ColumnarBatch],
                                 sampler: RandomSampler[ColumnarBatch, ColumnarBatch],
                                 preservesPartitioning: Boolean,
                                 @transient private val seed: Long = Utils.random.nextLong)
  extends PartitionwiseSampledRDD[ColumnarBatch, ColumnarBatch](prev, sampler,
    preservesPartitioning, seed) {
}
