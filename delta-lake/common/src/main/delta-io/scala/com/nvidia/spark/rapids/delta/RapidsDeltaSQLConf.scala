/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 *
 * This file was derived from DeltaSQLConf.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 * (pending at https://github.com/delta-io/delta/pull/1198).
*
 * Copyright (2021) The Delta Lake Project Authors.
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

package com.nvidia.spark.rapids.delta

import org.apache.spark.sql.delta.sources.DeltaSQLConf

/** Delta Lake related configs that are not yet provided by Delta Lake. */
trait RapidsDeltaSQLConf {
  val OPTIMIZE_WRITE_SMALL_PARTITION_FACTOR =
    DeltaSQLConf.buildConf("optimizeWrite.smallPartitionFactor")
        .internal()
        .doc("Factor used to coalesce partitions for optimize write.")
        .doubleConf
        .createWithDefault(0.5)

  val OPTIMIZE_WRITE_MERGED_PARTITION_FACTOR =
    DeltaSQLConf.buildConf("optimizeWrite.mergedPartitionFactor")
        .internal()
        .doc("Factor used to rebalance partitions for optimize write.")
        .doubleConf
        .createWithDefault(1.2)
}

object RapidsDeltaSQLConf extends RapidsDeltaSQLConf
