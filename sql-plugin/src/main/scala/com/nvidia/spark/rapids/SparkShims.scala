/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import org.apache.hadoop.fs.FileStatus
import org.apache.parquet.schema.MessageType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession => SqlSparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, CreateMap, Expression,
  MapConcat, MapFromArrays, MapFromEntries, ScalaUDF, StringToMap}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.command.{DataWritingCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources.{FileFormat, FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

trait SparkShims {
  def parquetRebaseReadKey: String
  def parquetRebaseWriteKey: String
  def avroRebaseReadKey: String
  def avroRebaseWriteKey: String
  def parquetRebaseRead(conf: SQLConf): String
  def parquetRebaseWrite(conf: SQLConf): String
  def v1RepairTableCommand(tableName: TableIdentifier): RunnableCommand
  def int96ParquetRebaseRead(conf: SQLConf): String
  def int96ParquetRebaseWrite(conf: SQLConf): String
  def int96ParquetRebaseReadKey: String
  def int96ParquetRebaseWriteKey: String
  def isCastingStringToNegDecimalScaleSupported: Boolean = true

  // These expressions are stateful only because they reuse mutable scratch buffers. Bridge
  // projection cloning gives every worker its own expression tree and therefore its own buffers.
  protected def isBridgeCloneSafeStatefulExpression(expr: Expression): Boolean = expr match {
    case _: ScalaUDF | _: CreateMap | _: MapFromArrays | _: MapConcat |
        _: MapFromEntries | _: StringToMap => true
    case _ => false
  }

  def isExpressionStateful(expr: Expression): Boolean = false

  def getParquetFilters(
    schema: MessageType,
    pushDownDate: Boolean,
    pushDownTimestamp: Boolean,
    pushDownDecimal: Boolean,
    pushDownStartWith: Boolean,
    pushDownInFilterThreshold: Int,
    caseSensitive: Boolean,
    lookupFileMeta: String => String,
    dateTimeRebaseModeFromConf: String): ParquetFilters

  def isWindowFunctionExec(plan: SparkPlan): Boolean
  def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]]
  def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]]
  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]]
  def getPartitionings: Map[Class[_ <: Partitioning], PartRule[_ <: Partitioning]] = Map.empty
  def getDataWriteCmds: Map[Class[_ <: DataWritingCommand],
    DataWritingCommandRule[_ <: DataWritingCommand]]
  def getRunnableCmds: Map[Class[_ <: RunnableCommand], RunnableCommandRule[_ <: RunnableCommand]]

  def newBroadcastQueryStageExec(
      old: BroadcastQueryStageExec,
      newPlan: SparkPlan): BroadcastQueryStageExec

  def getFileScanRDD(
      sparkSession: SqlSparkSession,
      readFunction: (PartitionedFile) => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference] = Seq.empty,
      fileFormat: Option[FileFormat] = None): RDD[InternalRow]

  /**
   * Get files from FilePartition. In Spark 4.0+, the files method was deprecated
   * in favor of filesWithAbsolutePaths or innerFiles.
   */
  def getPartitionFiles(partition: FilePartition): Seq[PartitionedFile]

  def listPartitionsByFilter(
      sparkSession: SqlSparkSession,
      tableName: TableIdentifier,
      predicates: Seq[Expression],
      resolvedCatalogTable: Option[CatalogTable]): Seq[CatalogTablePartition]

  def listPartitions(
      sparkSession: SqlSparkSession,
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec],
      resolvedCatalogTable: Option[CatalogTable]): Seq[CatalogTablePartition]

  def shouldFailDivOverflow: Boolean

  def reusedExchangeExecPfn: PartialFunction[SparkPlan, ReusedExchangeExec]

  /** dropped by SPARK-34234 */
  def attachTreeIfSupported[TreeType <: TreeNode[_], A](
    tree: TreeType,
    msg: String = "")(
    f: => A
  ): A

  def hasAliasQuoteFix: Boolean

  def hasCastFloatTimestampUpcast: Boolean

  def filesFromFileIndex(fileCatalog: PartitioningAwareFileIndex): Seq[FileStatus]

  def isEmptyRelation(relation: Any): Boolean

  def broadcastModeTransform(mode: BroadcastMode, toArray: Array[InternalRow]): Any

  /**
   * This call can produce an `EmptyHashedRelation` or an empty array,
   * allowing the AQE rule `EliminateJoinToEmptyRelation` in Spark 3.1.x
   * to optimize certain joins.
   *
   * In Spark 3.2.0, the optimization is still performed (under `AQEPropagateEmptyRelation`),
   * but the AQE optimizer is looking at the metrics for the query stage to determine
   * if numRows == 0, and if so it can eliminate certain joins.
   *
   * The call is implemented only for Spark 3.1.x+. It is disabled in
   * Databricks because it requires a task context to perform the
   * `BroadcastMode.transform` call, but we'd like to call this from the driver.
   */
  def tryTransformIfEmptyRelation(mode: BroadcastMode): Option[Any]

  def isAqePlan(p: SparkPlan): Boolean

  def isExchangeOp(plan: SparkPlanMeta[_]): Boolean

  def getDateFormatter(): DateFormatter

  def isCustomReaderExec(x: SparkPlan): Boolean

  def aqeShuffleReaderExec: ExecRule[_ <: SparkPlan]

  def isExecutorBroadcastShuffle(shuffle: ShuffleExchangeLike): Boolean = false

  def shuffleParentReadsShuffleData(shuffle: ShuffleExchangeLike, parent: SparkPlan): Boolean =
    false

  /**
   * Adds a row-based shuffle to the transititonal shuffle query stage if needed. This
   * is needed when AQE plans a GPU shuffleexchange to be reused by a parent plan exec
   * that consumes rows
   */
  def addRowShuffleToQueryStageTransitionIfNeeded(c2r: ColumnarToRowTransition,
      sqse: ShuffleQueryStageExec): SparkPlan = c2r

  /*
   * The following two functions are used to recognize when an executor broadcast
   * is being used to feed into a join but a columnar to row gets inserted between
   * the exchange and the join. This causes issues on some versions of Spark so we
   * have to shim it.
   */
  def checkCToRWithExecBroadcastAQECoalPart(p: SparkPlan,
      parent: Option[SparkPlan]): Boolean = false

  def getShuffleFromCToRWithExecBroadcastAQECoalPart(p: SparkPlan): Option[SparkPlan] = None

  /**
   * If the shim doesn't support executor broadcast, just return the plan passed in
   */
  def addExecBroadcastShuffle(p: SparkPlan): SparkPlan = p

  /**
   * Walk the plan recursively and return a list of operators that match the predicate
   */
  def findOperators(plan: SparkPlan, predicate: SparkPlan => Boolean): Seq[SparkPlan]

  /**
   * Our tests, by default, will check that all operators are running on the GPU, but
   * there are some operators that we do not translate to GPU plans, so we need a way
   * to bypass the check for those.
   */
  def skipAssertIsOnTheGpu(plan: SparkPlan): Boolean

  def getAdaptiveInputPlan(adaptivePlan: AdaptiveSparkPlanExec): SparkPlan

  def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan]

  /**
   * Return the replacement rule for AnsiCast.
   * 'AnsiCast' is removed from Spark 3.4.0, so need to handle it separately.
   */
  def ansiCastRule: ExprRule[_ <: Expression]

  /**
   * Determine if the Spark version allows the supportsColumnar flag to be overridden
   * in AdaptiveSparkPlanExec. This feature was introduced in Spark 3.2 as part of
   * SPARK-35881.
   */
  def supportsColumnarAdaptivePlans: Boolean

  def columnarAdaptivePlan(a: AdaptiveSparkPlanExec, goal: CoalesceSizeGoal): SparkPlan

  def applyShimPlanRules(plan: SparkPlan, conf: RapidsConf): SparkPlan = plan

  def applyPostShimPlanRules(plan: SparkPlan): SparkPlan = plan

  /**
   * Handle regexp_replace inconsistency from https://issues.apache.org/jira/browse/SPARK-39107
   */
  def reproduceEmptyStringBug: Boolean


  /**
   * Handle TableCacheQueryStageExec for getNonQueryStagePlan.
   * Returns None for versions where TableCacheQueryStageExec doesn't exist.
   */
  def getTableCacheNonQueryStagePlan(plan: SparkPlan): Option[SparkPlan] = None

  /**
   * Return true if this shim registers GPU replacements for `WriteFilesExec`.
   * Used to keep higher-level feature gating (e.g. LORE) aligned with shim coverage.
   */
  def hasGpuWriteFiles: Boolean = false
}
