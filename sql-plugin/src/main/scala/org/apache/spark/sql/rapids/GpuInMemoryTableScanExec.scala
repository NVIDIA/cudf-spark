/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.ParquetCachedBatchSerializer
import com.nvidia.spark.rapids.{DataFromReplacementRule, ExecChecks, GpuExec, GpuMetric, RapidsConf, RapidsMeta, SparkPlanMeta}
import com.nvidia.spark.rapids.shims.{InMemoryTableScanExecLikeShim, ShimLeafExecNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

class InMemoryTableScanMeta(
    imts: InMemoryTableScanExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[InMemoryTableScanExec](imts, conf, parent, rule) {

  override def tagPlanForGpu(): Unit = {
    def stringifyTypeAttributeMap(groupedByType: Map[DataType, Set[String]]): String = {
      groupedByType.map { case (dataType, nameSet) =>
        dataType + " " + nameSet.mkString("[", ", ", "]")
      }.mkString(", ")
    }

    val supportedTypeSig = rule.getChecks.get.asInstanceOf[ExecChecks]
    val unsupportedTypes: Map[DataType, Set[String]] = imts.relation.output
        .filterNot(attr => supportedTypeSig.check.isSupportedByPlugin(attr.dataType))
        .groupBy(_.dataType)
        .mapValues(_.map(_.name).toSet).toMap

    val msgFormat = "unsupported data types in output: %s"
    if (unsupportedTypes.nonEmpty) {
      willNotWorkOnGpu(msgFormat.format(stringifyTypeAttributeMap(unsupportedTypes)))
    }
    if (!imts.relation.cacheBuilder.serializer
        .isInstanceOf[com.nvidia.spark.ParquetCachedBatchSerializer]) {
      willNotWorkOnGpu("ParquetCachedBatchSerializer is not being used")
      if (SQLConf.get.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER)
          .equals("com.nvidia.spark.ParquetCachedBatchSerializer")) {
        throw new IllegalStateException("Cache serializer failed to load! " +
            "Something went wrong while loading ParquetCachedBatchSerializer class")
      }
    }
  }
  /**
   * Convert InMemoryTableScanExec to a GPU enabled version.
   */
  override def convertToGpu(): GpuExec = {
    GpuInMemoryTableScanExec(imts.attributes, imts.predicates, imts.relation)
  }
}

case class GpuInMemoryTableScanExec(
   attributes: Seq[Attribute],
   predicates: Seq[Expression],
   @transient relation: InMemoryRelation) extends ShimLeafExecNode with
   GpuExec with InMemoryTableScanExecLikeShim {

  override val nodeName: String = {
    relation.cacheBuilder.tableName match {
      case Some(_) =>
        "Scan " + relation.cacheBuilder.cachedName
      case _ =>
        super.nodeName
    }
  }

  override def innerChildren: Seq[QueryPlan[_]] = Seq(relation) ++ super.innerChildren

  override def doCanonicalize(): SparkPlan =
    copy(attributes = attributes.map(QueryPlan.normalizeExpressions(_, relation.output)),
      predicates = predicates.map(QueryPlan.normalizeExpressions(_, relation.output)),
      relation = relation.canonicalized.asInstanceOf[InMemoryRelation])

  override def vectorTypes: Option[Seq[String]] =
    relation.cacheBuilder.serializer.vectorTypes(attributes, conf)

  private lazy val columnarInputRDD: RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val buffers = filteredCachedBatches()
    relation.cacheBuilder.serializer.asInstanceOf[ParquetCachedBatchSerializer]
      .gpuConvertCachedBatchToColumnarBatch(
        buffers,
        relation.output,
        attributes,
        conf).map { cb =>
      numOutputRows += cb.numRows()
      cb
    }
  }

  override def output: Seq[Attribute] = attributes

  private def updateAttribute(expr: Expression): Expression = {
    // attributes can be pruned so using relation's output.
    // E.g., relation.output is [id, item] but this scan's output can be [item] only.
    val attrMap = AttributeMap(relation.cachedPlan.output.zip(relation.output))
    expr.transform {
      case attr: Attribute => attrMap.getOrElse(attr, attr)
    }
  }

  // The cached version does not change the outputPartitioning of the original SparkPlan.
  // But the cached version could alias output, so we need to replace output.
  override def outputPartitioning: Partitioning = {
    relation.cachedPlan.outputPartitioning match {
      case e: Expression => updateAttribute(e).asInstanceOf[Partitioning]
      case other => other
    }
  }

  // The cached version does not change the outputOrdering of the original SparkPlan.
  // But the cached version could alias output, so we need to replace output.
  override def outputOrdering: Seq[SortOrder] =
    relation.cachedPlan.outputOrdering.map(updateAttribute(_).asInstanceOf[SortOrder])

  lazy val enableAccumulatorsForTest: Boolean = sparkSession.sessionState
      .conf.inMemoryTableScanStatisticsEnabled

  // Accumulators used for testing purposes
  lazy val readPartitions = sparkSession.sparkContext.longAccumulator
  lazy val readBatches = sparkSession.sparkContext.longAccumulator

  private def filteredCachedBatches() = {
    // Right now just return the batch without filtering
    relation.cacheBuilder.cachedColumnBuffers
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("This Exec only deals with Columnar Data")
  }

  protected override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    columnarInputRDD
  }
}
