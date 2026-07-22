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
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.GpuMetric

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioningLike, Partitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Spark 4.1+ implementation of union for GpuUnionExec.
 *
 * Uses partitioner-aware union (SPARK-52921) which groups partitions at corresponding
 * indices across child RDDs, preserving partition alignment. Falls back to concatenation 
 * if partition counts differ.
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
   * Returns the output partitionings of the children, with the attributes converted to
   * the first child's attributes at the same position.
   */
  private def prepareOutputPartitioning(children: Seq[SparkPlan]): Seq[Partitioning] = {
    val firstAttrs = children.head.output
    val attributesMap = children.tail.map(_.output).map { otherAttrs =>
      otherAttrs.zip(firstAttrs).map { case (attr, firstAttr) =>
        attr -> firstAttr
      }.toMap
    }

    val partitionings = children.map(_.outputPartitioning)
    val firstPartitioning = partitionings.head
    val otherPartitionings = partitionings.tail

    val convertedOtherPartitionings = otherPartitionings.zipWithIndex.map { case (p, idx) =>
      val attributeMap = attributesMap(idx)
      p match {
        case e: Expression =>
          e.transform {
            case a: Attribute if attributeMap.contains(a) =>
              attributeMap(a)
          }.asInstanceOf[Partitioning]
        case _ => p
      }
    }
    Seq(firstPartitioning) ++ convertedOtherPartitionings
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
   * Mirrors Spark's UnionExec.outputPartitioning behavior.
   */
  def getOutputPartitioning(
      children: Seq[SparkPlan],
      unionOutput: Seq[Attribute],
      conf: SQLConf): Partitioning = {
    if (children.isEmpty) {
      UnknownPartitioning(0)
    } else if (conf.getConf(SQLConf.UNION_OUTPUT_PARTITIONING)) {
      val partitionings = prepareOutputPartitioning(children)
      val compatible = partitionings.forall(comparePartitioning(_, partitionings.head))
      if (compatible) {
        val partitioner = partitionings.head

        // Take the output attributes of this union and map the partitioner to them.
        val attributeMap = children.head.output.zip(unionOutput).toMap
        partitioner match {
          case e: Expression =>
            e.transform {
              case a: Attribute if attributeMap.contains(a) => attributeMap(a)
            }.asInstanceOf[Partitioning]
          case _ => partitioner
        }
      } else {
        UnknownPartitioning(0)
      }
    } else {
      UnknownPartitioning(0)
    }
  }
}
