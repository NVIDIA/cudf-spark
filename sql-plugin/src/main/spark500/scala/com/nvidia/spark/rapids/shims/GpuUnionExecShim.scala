/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
{"spark":["500"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.GpuMetric

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioningLike, Partitioning, PartitioningCollection, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Spark 5.0 implementation of union for GpuUnionExec.
 *
 * Uses partitioner-aware union (SPARK-52921) which groups partitions at corresponding
 * indices across child RDDs, preserving partition alignment.
 *
 * Mirrors Spark 5.0 UnionExec behavior:
 * - SPARK-58317: Flattens each child's PartitioningCollection, intersects the
 *   index-co-locatable candidates (SinglePartition and HashPartitioningLike), and
 *   returns UnknownPartitioning(0), the single common candidate, or a PartitioningCollection.
 * - SPARK-58819: Maps every child's partitioning (including the first child) directly to
 *   union output attributes using AttributeMap (keyed by ExprId) before comparison,
 *   handling differences in qualifier, nullability, metadata, or name.
 */
object GpuUnionExecShim {
  def unionColumnarRdds(
      sc: SparkContext,
      rdds: Seq[RDD[ColumnarBatch]],
      outputPartitioning: Partitioning,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric): RDD[ColumnarBatch] = {
    if (outputPartitioning.isInstanceOf[UnknownPartitioning]) {
      sc.union(rdds).map { batch =>
        numOutputBatches += 1
        numOutputRows += batch.numRows
        batch
      }
    } else {
      val nonEmptyRdds = rdds.filter(!_.partitions.isEmpty)
      new GpuPartitionerAwareUnionRDD(sc, nonEmptyRdds, outputPartitioning.numPartitions,
        numOutputRows, numOutputBatches)
    }
  }

  /**
   * Returns the output partitionings of the children, with the attributes converted to this
   * union's output attributes at the same position using AttributeMap (keyed by ExprId).
   */
  private def prepareOutputPartitioning(
      children: Seq[SparkPlan],
      unionOutput: Seq[Attribute]): Seq[Partitioning] = {
    val attributesMaps = children.map(_.output).map { childAttrs =>
      AttributeMap(childAttrs.zip(unionOutput))
    }
    children.map(_.outputPartitioning).zip(attributesMaps).map { case (p, attributeMap) =>
      remapPartitioning(p, attributeMap)
    }
  }

  private def remapPartitioning(
      partitioning: Partitioning,
      attributeMap: AttributeMap[Attribute]): Partitioning = {
    partitioning match {
      case pc: PartitioningCollection =>
        PartitioningCollection(pc.partitionings.map(remapPartitioning(_, attributeMap)))
      case e: Expression =>
        e.transform {
          case a: Attribute if attributeMap.contains(a) =>
            attributeMap(a)
        }.asInstanceOf[Partitioning]
      case other => other
    }
  }

  /**
   * Flattens a partitioning into its leaf partitionings: a PartitioningCollection is
   * recursively replaced by its members, and any other partitioning yields itself.
   */
  private def flatten(partitioning: Partitioning): Seq[Partitioning] = partitioning match {
    case PartitioningCollection(partitionings) => partitionings.flatMap(flatten)
    case other => Seq(other)
  }

  private def comparePartitioning(left: Partitioning, right: Partitioning): Boolean = {
    (left, right) match {
      case (SinglePartition, SinglePartition) => true
      case (l: HashPartitioningLike, r: HashPartitioningLike) => l == r
      case _ => false
    }
  }

  /**
   * Returns the output partitioning for GpuUnionExec.
   * Mirrors Spark 5.0's UnionExec.outputPartitioning behavior.
   */
  def getOutputPartitioning(
      children: Seq[SparkPlan],
      unionOutput: Seq[Attribute],
      conf: SQLConf): Partitioning = {
    if (children.isEmpty) {
      UnknownPartitioning(0)
    } else if (conf.getConf(SQLConf.UNION_OUTPUT_PARTITIONING)) {
      val partitionings = prepareOutputPartitioning(children, unionOutput)

      // Treat each child's partitioning as a set of candidate partitionings (a
      // PartitioningCollection flattens to its members; a single partitioning is a one-element
      // set) and pass through the intersection across all children. Only index-co-locatable
      // partitionings participate (SinglePartition and HashPartitioningLike).
      val candidateSets = partitionings.map { p =>
        flatten(p).filter {
          case _: HashPartitioningLike => true
          case SinglePartition => true
          case _ => false
        }
      }

      val head = candidateSets.head
      val intersection = head.filter { c =>
        candidateSets.tail.forall(_.exists(comparePartitioning(c, _)))
      }

      val distinctIntersection = intersection.distinct
      distinctIntersection match {
        case Seq() => UnknownPartitioning(0)
        case Seq(p) => p
        case ps => PartitioningCollection(ps)
      }
    } else {
      UnknownPartitioning(0)
    }
  }
}
