/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.connector.write.V1Write
import org.apache.spark.sql.execution.datasources.v2.{LeafV2CommandExec, SupportsV1Write}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU version of AppendDataExecV1
 *
 * Physical plan node for append into a v2 table using V1 write interfaces.
 *
 * Rows in the output data set are appended.
 */
case class GpuAppendDataExecV1(
    table: SupportsWrite,
    plan: LogicalPlan,
    refreshCache: () => Unit,
    write: V1Write) extends GpuV1FallbackWriters

/**
 * GPU version of OverwriteByExpressionExecV1
 *
 * Physical plan node for overwrite into a v2 table with V1 write interfaces. Note that when this
 * interface is used, the atomicity of the operation depends solely on the target data source.
 *
 * Overwrites data in a table matched by a set of filters. Rows matching all of the filters will be
 * deleted and rows in the output data set are appended.
 *
 * This plan is used to implement SaveMode.Overwrite. The behavior of SaveMode.Overwrite is to
 * truncate the table -- delete all rows -- and append the output data set. This uses the filter
 * AlwaysTrue to delete all rows.
 */
case class GpuOverwriteByExpressionExecV1(
    table: SupportsWrite,
    plan: LogicalPlan,
    refreshCache: () => Unit,
    write: V1Write) extends GpuV1FallbackWriters

/** GPU version of V1FallbackWriters */
trait GpuV1FallbackWriters extends LeafV2CommandExec with SupportsV1Write with GpuExec {
  override def supportsColumnar: Boolean = false

  override def output: Seq[Attribute] = Nil

  def table: SupportsWrite

  def refreshCache: () => Unit

  def write: V1Write

  override def run(): Seq[InternalRow] = {
    writeWithV1(write.toInsertableRelation)
    refreshCache()
    Nil
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support" +
      s" mismatch:\n$this")
  }
}
