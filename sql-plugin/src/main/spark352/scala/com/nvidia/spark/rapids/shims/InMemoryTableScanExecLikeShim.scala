/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanLike}

/**
 * Shim trait that extends InMemoryTableScanLike for Spark 3.5.2+.
 * InMemoryTableScanLike trait was introduced in Spark 3.5.2.
 */
trait InMemoryTableScanExecLikeShim extends InMemoryTableScanLike with LeafExecNode {
  def attributes: Seq[Attribute]
  def predicates: Seq[Expression]  
  def relation: InMemoryRelation

  override def isMaterialized: Boolean = relation.cacheBuilder.isCachedColumnBuffersLoaded

  override def baseCacheRDD(): RDD[CachedBatch] = {
    relation.cacheBuilder.cachedColumnBuffers
  }

  override def runtimeStatistics: Statistics = {
    relation.computeStats()
  }
}
