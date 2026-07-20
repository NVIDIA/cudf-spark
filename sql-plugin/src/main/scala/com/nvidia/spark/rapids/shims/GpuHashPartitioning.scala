/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{GpuHashPartitioningBase, HashMode, Murmur3Mode}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, StatefulOpClusteredDistribution}

case class GpuHashPartitioning(expressions: Seq[Expression], numPartitions: Int,
    hashMode: HashMode = Murmur3Mode)
  extends GpuHashPartitioningBase(expressions, numPartitions, hashMode) {

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case h: StatefulOpClusteredDistribution =>
          expressions.length == h.expressions.length && expressions.zip(h.expressions).forall {
            case (l, r) => l.semanticEquals(r)
          }
        case c @ ClusteredDistribution(requiredClustering, requireAllClusterKeys, _) =>
          if (requireAllClusterKeys) {
            c.areAllClusterKeysMatched(expressions)
          } else {
            expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }          
        case _ => false
      }
    }
  }
}

object GpuHashPartitioning {
  def getDistribution(exprs: Seq[Expression]): Distribution = ClusteredDistribution(exprs)
}
