/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.rapids.GpuFileSourceScanExec

/**
 * Enable eager I/O prefetch for bucket scan nodes which are directly connected to a Bucket Join
 * node. By doing this, the scan nodes backed by MultiFileCloudPartitionReader can start
 * asynchronous reading tasks right after the initialization rather than waiting for the request
 * of first batch which being triggered by `iterator.next()`
 *
 * NOTE: This is postShimPlanRule which should be applied after GpuOverrides.
 */
object BucketJoinTwoSidesPrefetch extends Rule[SparkPlan] {

  // Traverse through the plan tree and enable IO prefetch for all GpuFileSourceScanExec
  // which are directly connected to this join node without any shuffle.
  private def enablePrefetchRecursively(p: SparkPlan): Unit = {
    p match {
      // stop forwarding when we hit an Exchange node (not only GpuExchanges)
      case _: Exchange =>
      // enable the prefetch for this scan node
      case scan: GpuFileSourceScanExec =>
        scan.applyEagerPrefetch()
      case _ =>
        p.children.foreach(enablePrefetchRecursively)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    // Enable IO prefetch by a mutable operation on target nodes instead of re-generating
    // the plan tree. By doing so, it saves a lot of trouble.
    if (RapidsConf.BUCKET_JOIN_IO_PREFETCH.get(plan.conf)) {
      // Firstly, find all sized join nodes.
      val sizedJoins = plan.collect {
        case sizedJoin: GpuShuffledSizedHashJoinExec[_] => sizedJoin
      }
      // Then, go through their children to find bucket scan nodes.
      sizedJoins.foreach(enablePrefetchRecursively)
    }
    plan
  }
}
