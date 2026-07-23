/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Portions of this class have been taken from DeltaTableV2 class
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

import com.databricks.sql.execution.metric.IncrementMetric
import com.databricks.sql.transaction.tahoe.{DeltaConfigs, DeltaOptions}
import com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat
import com.databricks.sql.transaction.tahoe.commands.{OptimizeTableCommand,
  OptimizeTableCommandEdge, WriteIntoDeltaCommand, WriteIntoDeltaEdge}
import com.databricks.sql.transaction.tahoe.coordinatedcommits.{
  CatalogOwnedTableUtils,
  CoordinatedCommitsUtils
}
import com.databricks.sql.transaction.tahoe.rapids.{
  GpuCheckOverflowInTableWrite,
  GpuDeltaLog,
  GpuDeltaV1Write,
  GpuWriteIntoDelta,
  GpuWriteIntoDeltaCommandMeta
}
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta.shims.DeltaLogShim
import com.nvidia.spark.rapids.shims.ShimPredicateHelper

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{
  And,
  AttributeReference,
  EqualTo,
  Expression,
  If,
  IsNotNull,
  Literal,
  Not
}
import org.apache.spark.sql.catalyst.plans.logical.TableSpec
import org.apache.spark.sql.connector.write.V1Write
import org.apache.spark.sql.execution.{
  ColumnarToRowExec,
  FileSourceScanExec,
  FilterExec,
  ProjectExec,
  RowToColumnarExec,
  SparkPlan
}
import org.apache.spark.sql.execution.command.{DataWritingCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.{
  AtomicCreateTableAsSelectExec,
  AtomicReplaceTableAsSelectExec
}
import org.apache.spark.sql.rapids.{GpuAnd, GpuEqualTo, GpuFileSourceScanExec, GpuNot}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType

object DeltaSpark400DB173Provider extends DatabricksDeltaProviderBase {

  override def getDataWritingCommandRules: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] = {
    Seq(
      GpuOverrides.dataWriteCmd[WriteIntoDeltaCommand](
        "Write files for a DBR Delta transaction",
        (a, conf, p, r) => new GpuWriteIntoDeltaCommandMeta(a, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[DataWritingCommand]), r)).toMap
  }
  
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val rule = GpuCheckOverflowInTableWrite.exprRule
    super.getExprs + (rule.getClassFor.asSubclass(classOf[Expression]) -> rule)
  }

  override def getRunnableCommandRules: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = {
    super.getRunnableCommandRules ++ Seq(
      GpuOverrides.runnableCmd[OptimizeTableCommand](
        "Optimize a Delta Lake table",
        (a, conf, p, r) => new OptimizeTableCommandMeta(a, conf, p, r))
          .disabledByDefault("Delta Lake optimize support is experimental"),
      GpuOverrides.runnableCmd[OptimizeTableCommandEdge](
        "Optimize a Delta Lake table",
        (a, conf, p, r) => new OptimizeTableCommandEdgeMeta(a, conf, p, r))
          .disabledByDefault("Delta Lake optimize support is experimental")
    ).map(r => (r.getClassFor.asSubclass(classOf[RunnableCommand]), r)).toMap
  }

  override def isSupportedFormat(format: Class[_ <: FileFormat]): Boolean =
    super.isSupportedFormat(format) ||
      format == classOf[GpuDeltaParquetFileFormat] ||
      format == classOf[GpuDeltaParquetFileFormatNativeDV]

  override def getReadFileFormat(
      relation: HadoopFsRelation, rapidsConf: RapidsConf): FileFormat = {
    val fmt = relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
    if (isPushDVPredicateDownEnabled(rapidsConf)) {
      GpuDeltaParquetFileFormatNativeDV(
        relation = relation,
        protocol = fmt.protocol,
        metadata = fmt.metadata,
        generateRowIndexFilterId = fmt.generateRowIndexFilterId,
        generateRowIndexFilterColumn = fmt.generateRowIndexFilterColumn,
        nullableRowTrackingConstantFields = fmt.nullableRowTrackingConstantFields,
        nullableRowTrackingGeneratedFields = fmt.nullableRowTrackingGeneratedFields,
        optimizationsEnabled = fmt.optimizationsEnabled,
        tablePath = fmt.tablePath,
        isCDCRead = fmt.isCDCRead)
    } else {
      GpuDeltaParquetFileFormat.convertToGpu(relation)
    }
  }

  override def isPushDVPredicateDownEnabled(conf: RapidsConf): Boolean = {
    val dvConf = DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX
    val useMetadataRowIndex = conf.getStr(dvConf.key)
      .getOrElse(dvConf.defaultValueString).toBoolean
    // Delta deletion-vector filtering is row-position based in both DBR modes. The native GPU
    // pushdown below only handles the DBR metadata-row-index plan shape, where Spark provides the
    // temporary row-index metadata column that we can prune after moving DV filtering into cuDF.
    useMetadataRowIndex && conf.isDeltaDeletionVectorPredicatePushdownEnabled
  }

  override def tryPushDVPredicateDownToScan(plan: SparkPlan): SparkPlan = {
    val pushed = DB173DVPredicatePushdown.pushToScan(plan)
    DB173DVPredicatePushdown.mergeIdenticalProjects(pushed)
  }

  override def pruneFileMetadata(plan: SparkPlan): SparkPlan = {
    def pruneMetadataProject(
        project: GpuProjectExec,
        scan: GpuFileSourceScanExec): SparkPlan = {
      project.copy(projectList = project.projectList.filterNot(_.name == "_metadata"))
        .withNewChildren(Seq(
          scan.copy(
            originalOutput = scan.originalOutput.filterNot(_.name == "_tmp_metadata_row_index"),
            requiredSchema = StructType(
              scan.requiredSchema.filterNot(_.name == "_tmp_metadata_row_index")
            ))(scan.rapidsConf)))
    }

    plan match {
      case dvRoot @ GpuProjectExec(outputList,
      dvFilter @ GpuFilterExec(condition,
      dvFilterInput @ GpuProjectExec(inputList, fsse: GpuFileSourceScanExec, _)), _)
          if condition.references.exists(ref => isDeletionVectorSkipRowColumn(ref.name)) &&
          !outputList.flatMap(_.references).exists(_.name == "_metadata") &&
          inputList.exists(_.name == "_metadata") =>
        dvRoot.withNewChildren(Seq(
          dvFilter.withNewChildren(Seq(
            pruneMetadataProject(dvFilterInput, fsse)))))
      // Defensive match for a CPU FilterExec shape before GPU/CPU transitions are inserted.
      case dvRoot @ GpuProjectExec(outputList,
      dvFilter @ FilterExec(condition,
      dvFilterInput @ GpuProjectExec(inputList, fsse: GpuFileSourceScanExec, _)), _)
          if condition.references.exists(ref => isDeletionVectorSkipRowColumn(ref.name)) &&
          !outputList.flatMap(_.references).exists(_.name == "_metadata") &&
          inputList.exists(_.name == "_metadata") =>
        dvRoot.withNewChildren(Seq(
          dvFilter.withNewChildren(Seq(
            pruneMetadataProject(dvFilterInput, fsse)))))
      case dvRoot @ GpuProjectExec(outputList,
      rowToCol @ RowToColumnarExec(
      dvFilter @ FilterExec(condition,
      colToRow @ ColumnarToRowExec(
      dvFilterInput @ GpuProjectExec(inputList, fsse: GpuFileSourceScanExec, _)))), _)
          if condition.references.exists(ref => isDeletionVectorSkipRowColumn(ref.name)) &&
          !outputList.flatMap(_.references).exists(_.name == "_metadata") &&
          inputList.exists(_.name == "_metadata") =>
        dvRoot.withNewChildren(Seq(
          rowToCol.withNewChildren(Seq(
            dvFilter.withNewChildren(Seq(
              colToRow.withNewChildren(Seq(
                pruneMetadataProject(dvFilterInput, fsse)))))))))
      case _ =>
        plan.withNewChildren(plan.children.map(pruneFileMetadata))
    }
  }

  override def isDVScan(meta: SparkPlanMeta[FileSourceScanExec]): Boolean = {
    val maybeDVScan = meta.parent.flatMap(_.parent).flatMap(_.parent).map(_.wrapped)
    maybeDVScan.exists {
      case ProjectExec(outputList, FilterExec(condition, ProjectExec(inputList, _))) =>
        condition.references.exists(ref => isDeletionVectorSkipRowColumn(ref.name)) &&
          inputList.exists(_.name == "_metadata") &&
          !outputList.flatMap(_.references).exists(_.name == "_metadata")
      case _ => false
    }
  }

  def isDmlMetricDVScan(meta: SparkPlanMeta[FileSourceScanExec]): Boolean = {
    def hasMetricSkipRowFilter(parent: Option[RapidsMeta[_, _, _]]): Boolean = {
      parent.exists { p =>
        p.wrapped match {
          case FilterExec(condition, _) =>
            condition.references.exists(ref => isDeletionVectorSkipRowColumn(ref.name)) &&
              condition.exists(_.isInstanceOf[IncrementMetric])
          case _ =>
            hasMetricSkipRowFilter(p.parent)
        }
      }
    }

    hasMetricSkipRowFilter(meta.parent)
  }

  private def isDeletionVectorSkipRowColumn(name: String): Boolean =
    name == DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME ||
      name == GpuDeltaParquetFileFormat.EDGE_COMPUTED_COLUMN_SKIP_ROW

  override protected def toGpuWrite(
     writeConfig: DeltaWriteV1Config,
     rapidsConf: RapidsConf): V1Write = new GpuDeltaV1Write {
    override def toInsertableRelation(): InsertableRelation = {
      new InsertableRelation {
        override def insert(data: DataFrame, overwrite: Boolean): Unit = {
          val session = data.sparkSession
          val deltaLog = writeConfig.deltaLog

          // The V1 write adapter does not carry DB-17.3 transaction metadata options.
          // Seed WriteIntoDeltaEdge from the current snapshot, matching existing DB shims.
          val cpuWrite = WriteIntoDeltaEdge(
            deltaLog,
            if (writeConfig.forceOverwrite) SaveMode.Overwrite else SaveMode.Append,
            new DeltaOptions(writeConfig.options.toMap, session.sessionState.conf),
            Nil,
            DeltaLogShim.getMetadata(deltaLog).configuration,
            data)
          val gpuWrite = GpuWriteIntoDelta(new GpuDeltaLog(deltaLog, rapidsConf), cpuWrite)
          gpuWrite.run(session)

          // Match InsertInto behavior by refreshing cached plans that refer to this relation,
          // including the relation itself when it is cached.
          val classic = TrampolineConnectShims.getActiveSession
          classic.sharedState.cacheManager.recacheByPlan(
            classic, LogicalRelation(deltaLog.createRelation()))
        }
      }
    }
  }

  override def tagForGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): Unit = {
    tagDB173UnsupportedTableSpec(meta, cpuExec.tableSpec, cpuExec.session)
    super.tagForGpu(cpuExec, meta)
    if (meta.canThisBeReplaced) {
      meta.willNotWorkOnGpu(
        "Delta CTAS is not yet supported on GPU for DB-17.3")
    }
  }

  override def tagForGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): Unit = {
    tagDB173UnsupportedTableSpec(meta, cpuExec.tableSpec, cpuExec.session)
    super.tagForGpu(cpuExec, meta)
    if (meta.canThisBeReplaced) {
      meta.willNotWorkOnGpu(
        "Delta RTAS is not yet supported on GPU for DB-17.3")
    }
  }

  // Keep CTAS/RTAS on CPU for DB-17.3 until GpuCreateDeltaTableCommand preserves the new
  // table-creation semantics. The feature-specific tags below make explain output name the
  // exact unsupported Delta feature instead of only the broad CTAS/RTAS fallback:
  //   - row filter / column mask             -> https://github.com/NVIDIA/spark-rapids/issues/14601
  //   - catalog-owned / coordinated commits  -> https://github.com/NVIDIA/spark-rapids/issues/14601
  //   - liquid clustering / auto TTL         -> https://github.com/NVIDIA/spark-rapids/issues/14599
  private def tagDB173UnsupportedTableSpec(
      meta: RapidsMeta[_, _, _],
      tableSpec: TableSpec,
      spark: SparkSession): Unit = {
    if (tableSpec.rowFilter.isDefined) {
      meta.willNotWorkOnGpu("Delta CTAS/RTAS with a row filter is not supported on GPU")
    }
    if (tableSpec.columnMasks.isDefined) {
      meta.willNotWorkOnGpu("Delta CTAS/RTAS with column masks is not supported on GPU")
    }
    if (tableSpec.clusterSpec.isDefined) {
      meta.willNotWorkOnGpu("Delta CTAS/RTAS with liquid clustering is not supported on GPU")
    }
    if (tableSpec.autoTTLSpec.isDefined) {
      meta.willNotWorkOnGpu("Delta CTAS/RTAS with auto-TTL is not supported on GPU")
    }
    val hasCatalogOwnedConfig =
      CatalogOwnedTableUtils.shouldEnableCatalogOwned(spark, tableSpec.properties, false)
    val hasDefaultCatalogOwnedConfig = CatalogOwnedTableUtils.defaultCatalogOwnedEnabled(spark)
    if (hasCatalogOwnedConfig || hasDefaultCatalogOwnedConfig) {
      meta.willNotWorkOnGpu(
        "Delta CTAS/RTAS for a catalog-owned table is not supported on GPU")
    }
    val ccKeys = CoordinatedCommitsUtils.TABLE_PROPERTY_KEYS.toSet
    val hasExplicitCCConfig =
      tableSpec.properties.keysIterator.exists(key => containsKeyIgnoreCase(ccKeys, key))
    val hasDefaultCCConfig =
      CoordinatedCommitsUtils.getDefaultCCConfigurations(spark, true).nonEmpty
    if (hasExplicitCCConfig || hasDefaultCCConfig) {
      meta.willNotWorkOnGpu(
        "Delta CTAS/RTAS configuring coordinated commits is not supported on GPU")
    }
    // DB-17.3 can enable deletion vectors during table creation from either an explicit table
    // property or the session default-property conf. The CPU CreateDeltaTableCommand updates
    // metadata for that case; the GPU command does not yet, so keep the plan on CPU rather than
    // creating a table with DV unexpectedly disabled. Heuristic auto-enable cases are covered by
    // the broader create-command port tracked in
    // https://github.com/NVIDIA/spark-rapids/issues/14601.
    val dvConf = DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION
    val dvKeys = (dvConf.key +: dvConf.alternateKeys).toSet
    val dvExplicitlyEnabled = tableSpec.properties.collectFirst {
      case (key, value) if containsKeyIgnoreCase(dvKeys, key) => value
    }.exists(trueOrInvalidBoolean)
    val dvEnabledByDefaultConf =
      spark.conf.getOption(dvConf.defaultTablePropertyKey).exists(trueOrInvalidBoolean)
    if (dvExplicitlyEnabled || dvEnabledByDefaultConf) {
      meta.willNotWorkOnGpu(
        "Delta CTAS/RTAS that creates a table with Deletion Vectors enabled is not " +
          "supported on GPU")
    }
  }

  private def containsKeyIgnoreCase(keys: Set[String], key: String): Boolean =
    keys.exists(_.equalsIgnoreCase(key))

  private def trueOrInvalidBoolean(value: String): Boolean =
    !value.trim.equalsIgnoreCase("false")
}

private object DB173DVPredicatePushdown extends ShimPredicateHelper {

  def pushToScan(plan: SparkPlan): SparkPlan = {
    def isDeletionVectorSkipRowColumnEqualToFalse(left: Expression, right: Expression): Boolean = {
      isDeletionVectorSkipRowColumnRef(left) && isZeroOrFalseLiteral(right) ||
        isDeletionVectorSkipRowColumnRef(right) && isZeroOrFalseLiteral(left)
    }

    def isDVCondition(condition: Expression): Boolean = {
      condition match {
        case GpuEqualTo(left, right) =>
          isDeletionVectorSkipRowColumnEqualToFalse(left, right)
        case EqualTo(left, right) =>
          isDeletionVectorSkipRowColumnEqualToFalse(left, right)
        case GpuNot(child) =>
          isDeletionVectorSkipRowColumnRef(child)
        case Not(child) =>
          isDeletionVectorSkipRowColumnRef(child)
        case GpuIsNotNull(child) =>
          isDeletionVectorSkipRowColumnRef(child)
        case IsNotNull(child) =>
          isDeletionVectorSkipRowColumnRef(child)
        case GpuIf(predicateExpr, trueExpr, falseExpr) =>
          isDVCondition(predicateExpr) &&
            isDVCondition(trueExpr) &&
            !falseExpr.references.exists(ref => isDeletionVectorSkipRowColumn(ref.name))
        case If(predicateExpr, trueExpr, falseExpr) =>
          isDVCondition(predicateExpr) &&
            isDVCondition(trueExpr) &&
            !falseExpr.references.exists(ref => isDeletionVectorSkipRowColumn(ref.name))
        case _ => false
      }
    }

    def isDeletionVectorSkipRowColumnRef(expr: Expression): Boolean = {
      expr match {
        case attr: AttributeReference if isDeletionVectorSkipRowColumn(attr.name) => true
        case _ => false
      }
    }

    def isZeroOrFalseLiteral(expr: Expression): Boolean = {
      expr match {
        case GpuLiteral(value: Number, _) if value.longValue() == 0L => true
        case GpuLiteral(value: Boolean, _) if !value => true
        case Literal(value: Number, _) if value.longValue() == 0L => true
        case Literal(value: Boolean, _) if !value => true
        case _ => false
      }
    }

    def pruneDeletionVectorSkipRowColumn(plan: SparkPlan): SparkPlan = {
      plan.transformUp {
        case project @ ProjectExec(projectList, _) =>
          project.copy(projectList = projectList.filterNot(isDeletionVectorSkipRowColumnRef))
        case project @ GpuProjectExec(projectList, _, _) =>
          project.copy(projectList = projectList.filterNot(isDeletionVectorSkipRowColumnRef))
        case fsse: FileSourceScanExec =>
          fsse.copy(
            output = fsse.output.filterNot(attr => isDeletionVectorSkipRowColumn(attr.name)),
            requiredSchema = StructType(fsse.requiredSchema.filterNot(field =>
              isDeletionVectorSkipRowColumn(field.name))),
            // AQE expects expressions in dataFilters to exist in the output of the scan.
            // It will not reuse the stage of the scan otherwise. Since we are removing
            // the deletion-vector skip-row column from scan's output, remove the
            // corresponding filter from dataFilters as well.
            dataFilters = fsse.dataFilters.filterNot(isDVCondition))
        case fsse: GpuFileSourceScanExec =>
          fsse.copy(
            originalOutput = fsse.originalOutput.filterNot(attr =>
              isDeletionVectorSkipRowColumn(attr.name)),
            requiredSchema = StructType(fsse.requiredSchema.filterNot(field =>
              isDeletionVectorSkipRowColumn(field.name))),
            // AQE expects expressions in dataFilters to exist in the output of the scan.
            // It will not reuse the stage of the scan otherwise. Since we are removing
            // the deletion-vector skip-row column from scan's output, remove the
            // corresponding filter from dataFilters as well.
            dataFilters = fsse.dataFilters.filterNot(isDVCondition(_)))(fsse.rapidsConf)
      }
    }

    def hasNativeDeletionVectorGpuScan(plan: SparkPlan): Boolean = {
      // Only native GPU DV scans can replace the skip-row filter. DB DML bitmap-writing
      // plans may still need that filter even when the plugin is enabled.
      plan.exists {
        case fsse: FileSourceScanExec =>
          fsse.relation.fileFormat.isInstanceOf[GpuDeltaParquetFileFormatNativeDV]
        case fsse: GpuFileSourceScanExec =>
          fsse.relation.fileFormat.isInstanceOf[GpuDeltaParquetFileFormatNativeDV]
        case _ => false
      }
    }

    def rewriteFilter(
        condition: Expression,
        child: SparkPlan,
        combinePredicates: (Expression, Expression) => Expression,
        copyFilter: (Expression, SparkPlan) => SparkPlan): Option[SparkPlan] = {
      val conjuncts = splitConjunctivePredicates(condition)
      val (dvPredicates, otherPredicates) = conjuncts.partition { predicate =>
        predicate.references.size == 1 &&
          predicate.references.exists(ref => isDeletionVectorSkipRowColumn(ref.name)) &&
          isDVCondition(predicate)
      }
      val otherPredicatesReadingSkipRow = otherPredicates.exists { predicate =>
        predicate.references.exists(ref => isDeletionVectorSkipRowColumn(ref.name))
      }
      if (dvPredicates.nonEmpty &&
          !otherPredicatesReadingSkipRow &&
          hasNativeDeletionVectorGpuScan(child)) {
        val newChild = pruneDeletionVectorSkipRowColumn(child)
        Some(if (otherPredicates.isEmpty) {
          newChild
        } else {
          copyFilter(otherPredicates.reduce(combinePredicates), newChild)
        })
      } else {
        None
      }
    }

    plan.transformUp {
      case filter @ GpuFilterExec(condition, child)
          if condition.references.exists(ref => isDeletionVectorSkipRowColumn(ref.name)) =>
        rewriteFilter(condition, child, GpuAnd(_, _),
          (newCondition, newChild) => filter.copy(condition = newCondition,
            child = newChild)(filter.coalesceAfter)).getOrElse(filter)
      case filter @ FilterExec(condition, child)
          if condition.references.exists(ref => isDeletionVectorSkipRowColumn(ref.name)) =>
        rewriteFilter(condition, child, And(_, _),
          (newCondition, newChild) => filter.copy(condition = newCondition, child = newChild))
          .getOrElse(filter)
    }
  }

  def mergeIdenticalProjects(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case p @ GpuProjectExec(projList1,
      GpuProjectExec(projList2, child, enablePreSplit1), enablePreSplit2) =>
        val projSet1 = projList1.map(_.exprId).toSet
        val projSet2 = projList2.map(_.exprId).toSet
        if (projSet1 == projSet2) {
          GpuProjectExec(projList1, child, enablePreSplit1 && enablePreSplit2)
        } else {
          p
        }
    }
  }

  private def isDeletionVectorSkipRowColumn(name: String): Boolean =
    name == DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME ||
      name == GpuDeltaParquetFileFormat.EDGE_COMPUTED_COLUMN_SKIP_ROW
}
