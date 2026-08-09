/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg

import scala.reflect.ClassTag
import scala.util.Try

import com.nvidia.spark.rapids.{AppendDataExecMeta, AtomicCreateTableAsSelectExecMeta, AtomicReplaceTableAsSelectExecMeta, FileFormatChecks, GpuExec, GpuExpression, GpuRowToColumnarExec, GpuScan, IcebergFormatType, OverwriteByExpressionExecMeta, OverwritePartitionsDynamicExecMeta, RapidsConf, ScanMeta, ScanRule, ShimReflectionUtils, SparkPlanMeta, StaticInvokeMeta, TargetSize, WriteFileOp}
import com.nvidia.spark.rapids.iceberg.IcebergProviderBase.{checkChildPlan, columnarChildOf}
import com.nvidia.spark.rapids.shims.{ReplaceDataExecMeta, ReplaceDataExecShim, WriteDeltaExecMeta}
import org.apache.iceberg.spark.GpuTypeToSparkType.toSparkType
import org.apache.iceberg.spark.functions.{BucketFunction, DaysFunction, GpuBucketExpression, GpuDaysExpression, GpuHoursExpression, GpuMonthsExpression, GpuTruncateExpression, GpuYearsExpression, HoursFunction, MonthsFunction, TruncateFunction, YearsFunction}
import org.apache.iceberg.spark.source.{GpuSparkPositionDeltaWrite, GpuSparkScan, GpuSparkWrite}
import org.apache.iceberg.spark.source.GpuSparkPositionDeltaWrite.tableOf
import org.apache.iceberg.spark.supportsCatalog

import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec, GpuAppendDataExec, GpuOverwriteByExpressionExec, GpuOverwritePartitionsDynamicExec, GpuWriteDeltaExec, OverwriteByExpressionExec, OverwritePartitionsDynamicExec, ReplaceDataExec, WriteDeltaExec}
import org.apache.spark.sql.execution.datasources.v2.rapids.{GpuAtomicCreateTableAsSelectExec, GpuAtomicReplaceTableAsSelectExec}
import org.apache.spark.sql.types.{DateType, TimestampType}

abstract class IcebergProviderBase extends IcebergProvider {
  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = {
    val cpuBatchQueryScanClass = ShimReflectionUtils.loadClass(
      IcebergProvider.cpuBatchQueryScanClassName)
    val cpuCopyOnWriteScanClass = ShimReflectionUtils.loadClass(
      IcebergProvider.cpuCopyOnWriteScanClassName)

    Seq(
      new ScanRule[Scan](
        (a, conf, p, r) => new ScanMeta[Scan](a, conf, p, r) {
          private lazy val convertedScan: Try[GpuSparkScan] = GpuSparkScan.tryConvert(a, this.conf)

          override def supportsRuntimeFilters: Boolean = true

          override def tagSelfForGpu(): Unit = {
            GpuSparkScan.tagForGpu(this, convertedScan)
          }

          override def convertToGpu(): GpuScan = convertedScan.get
        },
        "Iceberg batch query scan",
        ClassTag(cpuBatchQueryScanClass)
      ),
      new ScanRule[Scan](
        (a, conf, p, r) => new ScanMeta[Scan](a, conf, p, r) {
          private lazy val convertedScan: Try[GpuSparkScan] = GpuSparkScan.tryConvert(a, this.conf)

          override def supportsRuntimeFilters: Boolean = true

          override def tagSelfForGpu(): Unit = {
            GpuSparkScan.tagForGpu(this, convertedScan)
          }

          override def convertToGpu(): GpuScan = convertedScan.get
        },
        "Iceberg copy on write scan",
        ClassTag(cpuCopyOnWriteScanClass)
      ),
    ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap
  }

  override def tagForGpu(expr: StaticInvoke, meta: StaticInvokeMeta): Unit = {
    if (classOf[BucketFunction.BucketBase].isAssignableFrom(expr.staticObject)) {
      GpuBucketExpression.tagExprForGpu(meta)
    } else if (classOf[YearsFunction.DateToYearsFunction].isAssignableFrom(expr.staticObject)) {
      GpuYearsExpression.tagExprForGpu(meta, DateType)
    } else if (
      classOf[YearsFunction.TimestampToYearsFunction].isAssignableFrom(expr.staticObject)) {
      GpuYearsExpression.tagExprForGpu(meta, TimestampType)
    } else if (classOf[MonthsFunction.DateToMonthsFunction].isAssignableFrom(expr.staticObject)) {
      GpuMonthsExpression.tagExprForGpu(meta, DateType)
    } else if (
      classOf[MonthsFunction.TimestampToMonthsFunction].isAssignableFrom(expr.staticObject)) {
      GpuMonthsExpression.tagExprForGpu(meta, TimestampType)
    } else if (classOf[DaysFunction.DateToDaysFunction].isAssignableFrom(expr.staticObject)) {
      GpuDaysExpression.tagExprForGpu(meta, DateType)
    } else if (classOf[DaysFunction.TimestampToDaysFunction].isAssignableFrom(expr.staticObject)) {
      GpuDaysExpression.tagExprForGpu(meta, TimestampType)
    } else if (
      classOf[HoursFunction.TimestampToHoursFunction].isAssignableFrom(expr.staticObject)) {
      GpuHoursExpression.tagExprForGpu(meta)
    } else if (classOf[TruncateFunction.TruncateInt].isAssignableFrom(expr.staticObject)) {
      GpuTruncateExpression.tagExprForGpu(meta)
    } else if (classOf[TruncateFunction.TruncateBigInt].isAssignableFrom(expr.staticObject)) {
      GpuTruncateExpression.tagExprForGpu(meta)
    } else if (classOf[TruncateFunction.TruncateString].isAssignableFrom(expr.staticObject)) {
      GpuTruncateExpression.tagExprForGpu(meta)
    } else if (classOf[TruncateFunction.TruncateBinary].isAssignableFrom(expr.staticObject)) {
      GpuTruncateExpression.tagExprForGpu(meta)
    } else if (classOf[TruncateFunction.TruncateDecimal].isAssignableFrom(expr.staticObject)) {
      GpuTruncateExpression.tagExprForGpu(meta)
    } else {
      meta.willNotWorkOnGpu(s"StaticInvoke of ${expr.staticObject.getName} is not supported on GPU")
    }
  }

  override def convertToGpu(expr: StaticInvoke, meta: StaticInvokeMeta): GpuExpression = {
    if (classOf[BucketFunction.BucketBase].isAssignableFrom(expr.staticObject)) {
      val Seq(left, right) = meta.childExprs.map(_.convertToGpu())
      GpuBucketExpression(left, right)
    } else if (classOf[YearsFunction.DateToYearsFunction].isAssignableFrom(expr.staticObject)) {
      GpuYearsExpression(meta.childExprs.head.convertToGpu())
    } else if (
      classOf[YearsFunction.TimestampToYearsFunction].isAssignableFrom(expr.staticObject)) {
      GpuYearsExpression(meta.childExprs.head.convertToGpu())
    } else if (classOf[MonthsFunction.DateToMonthsFunction].isAssignableFrom(expr.staticObject)) {
      GpuMonthsExpression(meta.childExprs.head.convertToGpu())
    } else if (
      classOf[MonthsFunction.TimestampToMonthsFunction].isAssignableFrom(expr.staticObject)) {
      GpuMonthsExpression(meta.childExprs.head.convertToGpu())
    } else if (classOf[DaysFunction.DateToDaysFunction].isAssignableFrom(expr.staticObject)) {
      GpuDaysExpression(meta.childExprs.head.convertToGpu())
    } else if (classOf[DaysFunction.TimestampToDaysFunction].isAssignableFrom(expr.staticObject)) {
      GpuDaysExpression(meta.childExprs.head.convertToGpu())
    } else if (
      classOf[HoursFunction.TimestampToHoursFunction].isAssignableFrom(expr.staticObject)) {
      GpuHoursExpression(meta.childExprs.head.convertToGpu())
    } else if (classOf[TruncateFunction.TruncateInt].isAssignableFrom(expr.staticObject)) {
      val Seq(left, right) = meta.childExprs.map(_.convertToGpu())
      GpuTruncateExpression(left, right)
    } else if (classOf[TruncateFunction.TruncateBigInt].isAssignableFrom(expr.staticObject)) {
      val Seq(left, right) = meta.childExprs.map(_.convertToGpu())
      GpuTruncateExpression(left, right)
    } else if (classOf[TruncateFunction.TruncateString].isAssignableFrom(expr.staticObject)) {
      val Seq(left, right) = meta.childExprs.map(_.convertToGpu())
      GpuTruncateExpression(left, right)
    } else if (classOf[TruncateFunction.TruncateBinary].isAssignableFrom(expr.staticObject)) {
      val Seq(left, right) = meta.childExprs.map(_.convertToGpu())
      GpuTruncateExpression(left, right)
    } else if (classOf[TruncateFunction.TruncateDecimal].isAssignableFrom(expr.staticObject)) {
      val Seq(left, right) = meta.childExprs.map(_.convertToGpu())
      GpuTruncateExpression(left, right)
    } else {
      throw new IllegalStateException(
        s"Should have been caught in tagExprForGpu: ${expr.staticObject.getName}")
    }
  }

  override def isSupportedWrite(write: Class[_ <: Write]): Boolean = {
    GpuSparkWrite.supports(write)
  }

  override def isSupportedCatalog(catalogClass: Class[_]): Boolean = {
    supportsCatalog(catalogClass)
  }

  private def tagForGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG.key} to true")
    }

    if (!meta.conf.isIcebergWriteEnabled) {
      meta.willNotWorkOnGpu("Iceberg output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_WRITE.key} to true")
    }

    FileFormatChecks.tag(meta, cpuExec.query.schema, IcebergFormatType, WriteFileOp)

    GpuSparkWrite.tagForGpuCtas(cpuExec, meta)

    checkChildPlan(meta)
  }

  private def convertToGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): GpuExec = {
    GpuAtomicCreateTableAsSelectExec(
      cpuExec.catalog,
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.query,
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.ifNotExists)
  }

  private def tagForGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG.key} to true")
    }

    if (!meta.conf.isIcebergWriteEnabled) {
      meta.willNotWorkOnGpu("Iceberg output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_WRITE.key} to true")
    }

    FileFormatChecks.tag(meta, cpuExec.query.schema, IcebergFormatType, WriteFileOp)

    GpuSparkWrite.tagForGpuRtas(cpuExec, meta)

    checkChildPlan(meta)
  }

  private def convertToGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): GpuExec = {
    GpuAtomicReplaceTableAsSelectExec(
      cpuExec.catalog,
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.query,
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.orCreate,
      cpuExec.invalidateCache)
  }

  private def tagForGpu(cpuExec: AppendDataExec, meta: AppendDataExecMeta): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG} to true")
    }

    if (!meta.conf.isIcebergWriteEnabled) {
      meta.willNotWorkOnGpu("Iceberg output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_WRITE} to true")
    }

    FileFormatChecks.tag(meta, cpuExec.query.schema, IcebergFormatType, WriteFileOp)

    GpuSparkWrite.tagForGpu(cpuExec.write, meta)
  }

  private def convertToGpu(cpuExec: AppendDataExec, meta: AppendDataExecMeta): GpuExec = {
    GpuAppendDataExec(
      columnarChildOf(meta),
      cpuExec.refreshCache,
      GpuSparkWrite.convert(cpuExec.write))
  }

  private def tagForGpu(cpuExec: OverwritePartitionsDynamicExec,
                         meta: OverwritePartitionsDynamicExecMeta): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG} to true")
    }

    if (!meta.conf.isIcebergWriteEnabled) {
      meta.willNotWorkOnGpu("Iceberg output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_WRITE} to true")
    }

    FileFormatChecks.tag(meta, cpuExec.query.schema, IcebergFormatType, WriteFileOp)

    GpuSparkWrite.tagForGpu(cpuExec.write, meta)
  }

  private def convertToGpu(cpuExec: OverwritePartitionsDynamicExec,
                            meta: OverwritePartitionsDynamicExecMeta): GpuExec = {
    GpuOverwritePartitionsDynamicExec(
      columnarChildOf(meta),
      cpuExec.refreshCache,
      GpuSparkWrite.convert(cpuExec.write))
  }

  private def tagForGpu(cpuExec: OverwriteByExpressionExec,
                         meta: OverwriteByExpressionExecMeta): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG} to true")
    }

    if (!meta.conf.isIcebergWriteEnabled) {
      meta.willNotWorkOnGpu("Iceberg output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_WRITE} to true")
    }

    FileFormatChecks.tag(meta, cpuExec.query.schema, IcebergFormatType, WriteFileOp)

    GpuSparkWrite.tagForGpu(cpuExec.write, meta)
  }

  private def convertToGpu(cpuExec: OverwriteByExpressionExec,
                            meta: OverwriteByExpressionExecMeta): GpuExec = {
    GpuOverwriteByExpressionExec(
      columnarChildOf(meta),
      cpuExec.refreshCache,
      GpuSparkWrite.convert(cpuExec.write))
  }

  def tagForGpuPlan[P <: SparkPlan, M <: SparkPlanMeta[P]](cpuExec: P, meta: M): Unit = {
    cpuExec match {
      case replaceData: ReplaceDataExec =>
        tagForGpu(replaceData, meta.asInstanceOf[ReplaceDataExecMeta])
      case writeDelta: WriteDeltaExec =>
        tagForGpu(writeDelta, meta.asInstanceOf[WriteDeltaExecMeta])
      case appendData: AppendDataExec =>
        tagForGpu(appendData, meta.asInstanceOf[AppendDataExecMeta])
      case createTable: AtomicCreateTableAsSelectExec =>
        tagForGpu(createTable, meta.asInstanceOf[AtomicCreateTableAsSelectExecMeta])
      case replaceTable: AtomicReplaceTableAsSelectExec =>
        tagForGpu(replaceTable, meta.asInstanceOf[AtomicReplaceTableAsSelectExecMeta])
      case overwritePartitions: OverwritePartitionsDynamicExec =>
        tagForGpu(overwritePartitions, meta.asInstanceOf[OverwritePartitionsDynamicExecMeta])
      case overwriteByExpr: OverwriteByExpressionExec =>
        tagForGpu(overwriteByExpr, meta.asInstanceOf[OverwriteByExpressionExecMeta])
      case _ =>
        meta.willNotWorkOnGpu(s"IcebergProviderImpl does not support ${cpuExec.getClass.getName}")
    }
  }

  def convertToGpuPlan[P <: SparkPlan, M <: SparkPlanMeta[P]](cpuExec: P, meta: M): GpuExec = {
    cpuExec match {
      case replaceData: ReplaceDataExec =>
        convertToGpu(replaceData, meta.asInstanceOf[ReplaceDataExecMeta])
      case writeDelta: WriteDeltaExec =>
        convertToGpu(writeDelta, meta.asInstanceOf[WriteDeltaExecMeta])
      case appendData: AppendDataExec =>
        convertToGpu(appendData, meta.asInstanceOf[AppendDataExecMeta])
      case createTable: AtomicCreateTableAsSelectExec =>
        convertToGpu(createTable, meta.asInstanceOf[AtomicCreateTableAsSelectExecMeta])
      case replaceTable: AtomicReplaceTableAsSelectExec =>
        convertToGpu(replaceTable, meta.asInstanceOf[AtomicReplaceTableAsSelectExecMeta])
      case overwritePartitions: OverwritePartitionsDynamicExec =>
        convertToGpu(overwritePartitions, meta.asInstanceOf[OverwritePartitionsDynamicExecMeta])
      case overwriteByExpr: OverwriteByExpressionExec =>
        convertToGpu(overwriteByExpr, meta.asInstanceOf[OverwriteByExpressionExecMeta])
      case _ =>
        throw new IllegalStateException(
          s"IcebergProviderImpl does not support ${cpuExec.getClass.getName}")
    }
  }

  private def tagForGpu(cpuExec: ReplaceDataExec, meta: ReplaceDataExecMeta): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG} to true")
    }

    if (!meta.conf.isIcebergWriteEnabled) {
      meta.willNotWorkOnGpu("Iceberg output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_WRITE} to true")
    }

    FileFormatChecks.tag(meta, cpuExec.query.schema, IcebergFormatType, WriteFileOp)

    GpuSparkWrite.tagForGpu(cpuExec.write, meta)
  }

  private def convertToGpu(cpuExec: ReplaceDataExec, meta: ReplaceDataExecMeta): GpuExec = {
    ReplaceDataExecShim.convertToGpu(
      cpuExec,
      columnarChildOf(meta),
      GpuSparkWrite.convert(cpuExec.write))
  }

  private def tagForGpu(cpuExec: WriteDeltaExec, meta: WriteDeltaExecMeta): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG} to true")
    }

    if (!meta.conf.isIcebergWriteEnabled) {
      meta.willNotWorkOnGpu("Iceberg output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_WRITE} to true")
    }

    val outputSchema = toSparkType(tableOf(cpuExec.write).schema())
    FileFormatChecks.tag(meta, outputSchema,
      IcebergFormatType, WriteFileOp)

    GpuSparkPositionDeltaWrite.tagForGpu(cpuExec.write, meta)
  }

  private def convertToGpu(cpuExec: WriteDeltaExec, meta: WriteDeltaExecMeta): GpuExec = {
    GpuWriteDeltaExec(
      columnarChildOf(meta),
      cpuExec.refreshCache,
      cpuExec.projections,
      GpuSparkPositionDeltaWrite.convert(cpuExec.write))
  }
}

object IcebergProviderBase {
  def checkChildPlan[T <: SparkPlan](meta: SparkPlanMeta[T]): Unit = {
    if (meta.childPlans.nonEmpty) {
      val childMeta = meta.childPlans.head
      if (!childMeta.wrapped.isInstanceOf[AdaptiveSparkPlanExec] && !childMeta.canThisBeReplaced) {
        meta.willNotWorkOnGpu("Because child can't run gpu")
      }
    }
  }

  /**
   * Returns the child of a V2 write exec in a form whose `executeColumnar()` works.
   * - A GpuExec child or a CPU `AdaptiveSparkPlanExec` (handled by
   *   `GpuV2TableWriteExec.finalQuery`) is returned as-is.
   * - Any other (row-based) plan is wrapped in `GpuRowToColumnarExec` so the GPU
   *   write can consume row-based sources like `RDDScanExec`.
   */
  def columnarChildOf[T <: SparkPlan](meta: SparkPlanMeta[T]): SparkPlan = {
    val converted = meta.childPlans.head.convertIfNeeded()
    converted match {
      case _: GpuExec => converted
      case _: AdaptiveSparkPlanExec => converted
      case _ =>
        GpuRowToColumnarExec(converted, TargetSize(meta.conf.gpuTargetBatchSizeBytes))
    }
  }
}
