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

/*** spark-rapids-shim-json-lines
{"spark":["330","350","400","420"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{FileFormat, FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, OverwriteByExpressionExec}
import org.apache.spark.sql.types.StructType

trait Spark330PlusShims extends Spark321PlusShims with Spark320PlusNonDBShims {

  override def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan] = null

  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference],
      fileFormat: Option[FileFormat]): RDD[InternalRow] = {
    new FileScanRDD(sparkSession, readFunction, filePartitions, readDataSchema, metadataColumns)
  }

  // GPU support ANSI interval types from 330
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] =
    super.getExprs ++ YearMonthIntervalShims.exprs ++ DayTimeIntervalShims.exprs ++
      RoundingShims.exprs

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    // Source-specific recognizers in ExternalSource apply their own type checks. Keep these
    // command rules hidden so the generated generic Exec matrix does not claim one signature for
    // sources with different write contracts.
    val appendDataRule = GpuOverrides.exec[AppendDataExec](
      "Append data into a datasource V2 table",
      ExecChecks.hiddenHack(),
      (p, conf, parent, r) => new AppendDataExecMeta(p, conf, parent, r))
    val overwriteByExpressionRule = GpuOverrides.exec[OverwriteByExpressionExec](
      "Overwrite data in a datasource V2 table",
      ExecChecks.hiddenHack(),
      (p, conf, parent, r) => new OverwriteByExpressionExecMeta(p, conf, parent, r))
    val v2WriteRules: Seq[(Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan])] = Seq(
      (appendDataRule.getClassFor.asSubclass(classOf[SparkPlan]), appendDataRule),
      (overwriteByExpressionRule.getClassFor.asSubclass(classOf[SparkPlan]),
        overwriteByExpressionRule))

    super.getExecs ++ PythonMapInArrowExecShims.execs ++ v2WriteRules.toMap
  }

}

// Fallback to the default definition of `deterministic`
trait GpuDeterministicFirstLastCollectShim extends Expression
