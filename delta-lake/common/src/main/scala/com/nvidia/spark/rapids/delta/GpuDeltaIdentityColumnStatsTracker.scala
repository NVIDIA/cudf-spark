/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

import scala.collection.mutable

import ai.rapids.cudf.ColumnView
import com.nvidia.spark.rapids.{GpuColumnVector, GpuScalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.delta.shims.ShimJsonUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, BoundReference, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuDeltaIdentityColumnStatsTracker(
    val dataSchema: Seq[Attribute],
    val identityStatsExpr: Expression,
    val identityColumnInfo: Seq[(String, Boolean)])
    extends GpuDeltaJobStatisticsTracker(
      dataSchema,
      identityStatsExpr,
      GpuDeltaIdentityColumnStatsTracker.batchStatsToRow(
        dataSchema,
        identityStatsExpr,
        identityColumnInfo)) {

  val highWaterMarks: mutable.Map[String, Long] = mutable.Map.empty

  override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {
    stats.map(_.asInstanceOf[GpuDeltaFileStatistics]).flatMap(_.stats).foreach { case (_, json) =>
      val marks = ShimJsonUtils.fromJson[Array[Long]](json)
      require(identityColumnInfo.size == marks.length)
      identityColumnInfo.zip(marks).foreach { case ((name, useMax), mark) =>
        val newMark = highWaterMarks.get(name).map { oldMark =>
          if (useMax) oldMark.max(mark) else oldMark.min(mark)
        }.getOrElse(mark)
        highWaterMarks.update(name, newMark)
      }
    }
  }
}

object GpuDeltaIdentityColumnStatsTracker {
  def batchStatsToRow(
      dataCols: Seq[Attribute],
      identityStatsExpr: Expression,
      identityInfo: Seq[(String, Boolean)]): (ColumnarBatch, InternalRow) => Unit = {
    val aggregates = identityStatsExpr.collect {
      case ae: DeclarativeAggregate => ae
    }
    val boundExprs = BindReferences.bindReferences(aggregates, dataCols)
    val boundRefs = boundExprs.map { expr =>
      assert(expr.children.size == 1, s"expected single child, found ${expr.children.size}")
      expr.children.head.asInstanceOf[BoundReference]
    }
    assert(identityInfo.size == boundRefs.size,
      s"expected ${identityInfo.size} refs found ${boundRefs.size}")
    val zipped = identityInfo.map(_._2).zip(boundRefs).zipWithIndex
    (batch: ColumnarBatch, row: InternalRow) => {
      val columnViews = GpuColumnVector.extractBases(batch).asInstanceOf[Array[ColumnView]]
      zipped.foreach { case ((useMax, ref), i) =>
        val cview = columnViews(ref.ordinal)
        val gpuScalar = if (useMax) {
          cview.max()
        } else {
          cview.min()
        }
        withResource(gpuScalar) { _ =>
          val scalar = GpuScalar.extract(gpuScalar)
          val valueType = if (scalar == null) NullType else dataCols(i).dataType
          val writer = InternalRow.getWriter(i, valueType)
          writer(row, scalar)
        }
      }
    }
  }
}
