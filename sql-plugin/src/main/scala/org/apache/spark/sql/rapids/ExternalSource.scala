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

package org.apache.spark.sql.rapids

import scala.reflect.ClassTag
import scala.util.Try

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta.DeltaProvider
import com.nvidia.spark.rapids.iceberg.IcebergProvider

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.command.{DataWritingCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, AppendDataExecV1, AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec, OverwriteByExpressionExec, OverwriteByExpressionExecV1, OverwritePartitionsDynamicExec}
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.util.Utils

/**
 * The subclass of AvroProvider imports spark-avro classes. This file should not imports
 * spark-avro classes because `class not found` exception may throw if spark-avro does not
 * exist at runtime. Details see: https://github.com/NVIDIA/spark-rapids/issues/5648
 */
trait ExternalSourceBase extends Logging {
  val avroScanClassName = "org.apache.spark.sql.v2.avro.AvroScan"
  lazy val hasSparkAvroJar = {
    /** spark-avro is an optional package for Spark, so the RAPIDS Accelerator
     * must run successfully without it. */
    Utils.classIsLoadable(avroScanClassName) && {
      Try(ShimReflectionUtils.loadClass(avroScanClassName)).map(_ => true)
        .getOrElse {
          logWarning("Avro library not found by the RAPIDS plugin. The Plugin jars are " +
              "likely deployed using a static classpath spark.driver/executor.extraClassPath. " +
              "Consider using --jars or --packages instead.")
          false
        }
    }
  }

  lazy val avroProvider = ShimLoaderTemp.newAvroProvider()

  lazy val hasIcebergJar = {
    IcebergProvider.isSupportedSparkVersion() &&
      Utils.classIsLoadable(IcebergProvider.cpuBatchQueryScanClassName) &&
        Try(ShimReflectionUtils.loadClass(IcebergProvider.cpuBatchQueryScanClassName)).isSuccess
  }

  protected lazy val icebergProvider = IcebergProvider()

  private lazy val deltaProvider = DeltaProvider()

  private lazy val creatableRelations = deltaProvider.getCreatableRelationRules

  lazy val runnableCmds: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = deltaProvider.getRunnableCommandRules

  lazy val dataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] =
    deltaProvider.getDataWritingCommandRules

  lazy val execRules: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    deltaProvider.getExecRules

  lazy val exprRules: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] =
    deltaProvider.getExprs

  /** If the file format is supported as an external source */
  def isSupportedFormat(format: Class[_ <: FileFormat]): Boolean = {
    if (hasSparkAvroJar && avroProvider.isSupportedFormat(format)) {
      true
    } else if (deltaProvider.isSupportedFormat(format)) {
      true
    } else {
      false
    }
  }

  def isSupportedWrite(write: Class[_ <: SupportsWrite]): Boolean = {
    deltaProvider.isSupportedWrite(write)
  }

  def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val format = meta.wrapped.relation.fileFormat
    if (hasSparkAvroJar && avroProvider.isSupportedFormat(format.getClass)) {
      avroProvider.tagSupportForGpuFileSourceScan(meta)
    } else if (deltaProvider.isSupportedFormat(format.getClass)) {
      deltaProvider.tagSupportForGpuFileSourceScan(meta)
    } else {
      meta.willNotWorkOnGpu(s"unsupported file format: ${format.getClass.getCanonicalName}")
    }
  }

  /**
   * Get a read file format for the input format.
   * Better to check if the format is supported first by calling 'isSupportedFormat'
   */
  def getReadFileFormat(relation: HadoopFsRelation, rapidsConf: RapidsConf): FileFormat = {
    val format = relation.fileFormat
    if (hasSparkAvroJar && avroProvider.isSupportedFormat(format.getClass)) {
      avroProvider.getReadFileFormat(format)
    } else if (deltaProvider.isSupportedFormat(format.getClass)) {
      deltaProvider.getReadFileFormat(relation, rapidsConf)
    } else {
      throw new IllegalArgumentException(s"${format.getClass.getCanonicalName} is not supported")
    }
  }

  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = {
    var scans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Map.empty
    if (hasSparkAvroJar) {
      scans = scans ++ avroProvider.getScans
    }
    if (hasIcebergJar) {
      scans = scans ++ icebergProvider.getScans
    }
    scans
  }

  def wrapCreatableRelationProvider[INPUT <: CreatableRelationProvider](
      provider: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): CreatableRelationProviderMeta[INPUT] = {
    creatableRelations.get(provider.getClass).map { r =>
      r.wrap(provider, conf, parent, r).asInstanceOf[CreatableRelationProviderMeta[INPUT]]
    }.getOrElse(new RuleNotFoundCreatableRelationProviderMeta(provider, conf, parent))
  }

  def toCreatableRelationProviderRule[INPUT <: CreatableRelationProvider](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], DataFromReplacementRule)
          => CreatableRelationProviderMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): CreatableRelationProviderRule[INPUT] = {
    require(desc != null)
    require(doWrap != null)
    new CreatableRelationProviderRule[INPUT](doWrap, desc, tag)
  }

  def tagForGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): Unit = {
    val catalogClass = cpuExec.catalog.getClass
    if (deltaProvider.isSupportedCatalog(catalogClass)) {
      deltaProvider.tagForGpu(cpuExec, meta)
    } else if (hasIcebergJar && icebergProvider.isSupportedCatalog(catalogClass)) {
      icebergProvider.tagForGpuPlan(cpuExec, meta)
    } else {
      meta.willNotWorkOnGpu(s"catalog $catalogClass is not supported")
    }
  }

  def convertToGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): GpuExec = {
    val catalogClass = cpuExec.catalog.getClass
    if (deltaProvider.isSupportedCatalog(catalogClass)) {
      deltaProvider.convertToGpu(cpuExec, meta)
    } else if (hasIcebergJar && icebergProvider.isSupportedCatalog(catalogClass)) {
      icebergProvider.convertToGpuPlan(cpuExec, meta)
    } else {
      throw new IllegalStateException("No GPU conversion")
    }
  }

  def tagForGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): Unit = {
    val catalogClass = cpuExec.catalog.getClass
    if (deltaProvider.isSupportedCatalog(catalogClass)) {
      deltaProvider.tagForGpu(cpuExec, meta)
    } else if (hasIcebergJar && icebergProvider.isSupportedCatalog(catalogClass)) {
      icebergProvider.tagForGpuPlan(cpuExec, meta)
    } else {
      meta.willNotWorkOnGpu(s"catalog $catalogClass is not supported")
    }
  }

  def convertToGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): GpuExec = {
    val catalogClass = cpuExec.catalog.getClass
    if (deltaProvider.isSupportedCatalog(catalogClass)) {
      deltaProvider.convertToGpu(cpuExec, meta)
    } else if (hasIcebergJar && icebergProvider.isSupportedCatalog(catalogClass)) {
      icebergProvider.convertToGpuPlan(cpuExec, meta)
    } else {
      throw new IllegalStateException("No GPU conversion")
    }
  }

  def tagForGpu(
      cpuExec: AppendDataExecV1,
      meta: AppendDataExecV1Meta): Unit = {
    val writeClass = cpuExec.table.getClass
    if (deltaProvider.isSupportedWrite(writeClass)) {
      deltaProvider.tagForGpu(cpuExec, meta)
    } else {
      meta.willNotWorkOnGpu(s"catalog $writeClass is not supported")
    }
  }

  def convertToGpu(
      cpuExec: AppendDataExecV1,
      meta: AppendDataExecV1Meta): GpuExec = {
    val writeClass = cpuExec.table.getClass
    if (deltaProvider.isSupportedWrite(writeClass)) {
      deltaProvider.convertToGpu(cpuExec, meta)
    } else {
      throw new IllegalStateException("No GPU conversion")
    }
  }

  def tagForGpu(
      cpuExec: OverwriteByExpressionExecV1,
      meta: OverwriteByExpressionExecV1Meta): Unit = {
    val writeClass = cpuExec.table.getClass
    if (deltaProvider.isSupportedWrite(writeClass)) {
      deltaProvider.tagForGpu(cpuExec, meta)
    } else {
      meta.willNotWorkOnGpu(s"catalog $writeClass is not supported")
    }
  }

  def convertToGpu(
      cpuExec: OverwriteByExpressionExecV1,
      meta: OverwriteByExpressionExecV1Meta): GpuExec = {
    val writeClass = cpuExec.table.getClass
    if (deltaProvider.isSupportedWrite(writeClass)) {
      deltaProvider.convertToGpu(cpuExec, meta)
    } else {
      throw new IllegalStateException("No GPU conversion")
    }
  }

  def tagForGpu(
    cpuExec: AppendDataExec,
    meta: AppendDataExecMeta): Unit = {
    val writeClass = cpuExec.write.getClass

    if (hasIcebergJar && icebergProvider.isSupportedWrite(writeClass)) {
      icebergProvider.tagForGpuPlan(cpuExec, meta)
    } else {
      meta.willNotWorkOnGpu(s"Append data $writeClass is not supported")
    }
  }

  def convertToGpu(
    cpuExec: AppendDataExec,
    meta: AppendDataExecMeta): GpuExec = {
    val writeClass = cpuExec.write.getClass

    if (hasIcebergJar && icebergProvider.isSupportedWrite(writeClass)) {
      icebergProvider.convertToGpuPlan(cpuExec, meta)
    } else {
      throw new IllegalStateException("No GPU conversion")
    }
  }

  def tagForGpu(
    cpuExec: OverwritePartitionsDynamicExec,
    meta: OverwritePartitionsDynamicExecMeta): Unit = {
    val writeClass = cpuExec.write.getClass

    if (hasIcebergJar && icebergProvider.isSupportedWrite(writeClass)) {
      icebergProvider.tagForGpuPlan(cpuExec, meta)
    } else {
      meta.willNotWorkOnGpu(s"Overwrite partitions dynamic $writeClass is not supported")
    }
  }

  def convertToGpu(
    cpuExec: OverwritePartitionsDynamicExec,
    meta: OverwritePartitionsDynamicExecMeta): GpuExec = {
    val writeClass = cpuExec.write.getClass

    if (hasIcebergJar && icebergProvider.isSupportedWrite(writeClass)) {
      icebergProvider.convertToGpuPlan(cpuExec, meta)
    } else {
      throw new IllegalStateException("No GPU conversion")
    }
  }

  def tagForGpu(
                 cpuExec: OverwriteByExpressionExec,
                 meta: OverwriteByExpressionExecMeta): Unit = {
    val writeClass = cpuExec.write.getClass

    if (hasIcebergJar && icebergProvider.isSupportedWrite(writeClass)) {
      icebergProvider.tagForGpuPlan(cpuExec, meta)
    } else {
      meta.willNotWorkOnGpu(s"Overwrite data $writeClass is not supported")
    }
  }

  def convertToGpu(
                    cpuExec: OverwriteByExpressionExec,
                    meta: OverwriteByExpressionExecMeta): GpuExec = {
    val writeClass = cpuExec.write.getClass

    if (hasIcebergJar && icebergProvider.isSupportedWrite(writeClass)) {
      icebergProvider.convertToGpuPlan(cpuExec, meta)
    } else {
      throw new IllegalStateException("No GPU conversion")
    }
  }


  def tagForGpu(expr: StaticInvoke, meta: StaticInvokeMeta): Unit = {
    if (hasIcebergJar) {
      icebergProvider.tagForGpu(expr, meta)
    } else {
      meta.willNotWorkOnGpu(s"StaticInvoke is not supported")
    }
  }

  def convertToGpu(expr: StaticInvoke, meta: StaticInvokeMeta): GpuExpression = {
    if (hasIcebergJar) {
      icebergProvider.convertToGpu(expr, meta)
    } else {
      throw new IllegalStateException("StaticInvoke is not supported")
    }
  }
}

object ExternalSource extends ExternalSourceBase {
}
