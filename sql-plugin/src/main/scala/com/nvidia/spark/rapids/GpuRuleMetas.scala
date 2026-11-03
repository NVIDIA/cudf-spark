/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.shims._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.json.rapids.GpuJsonScan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.rapids.execution._
import org.apache.spark.sql.types._

case class CSVScanRuleMeta(
    a: CSVScan,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ScanMeta[CSVScan](a, conf, p, r) {
  override def tagSelfForGpu(): Unit = GpuCSVScan.tagSupport(this)

  override def convertToGpu(): GpuScan =
    GpuCSVScan(a.sparkSession,
      a.fileIndex,
      a.dataSchema,
      a.readDataSchema,
      a.readPartitionSchema,
      a.options,
      a.partitionFilters,
      a.dataFilters,
      this.conf.maxReadBatchSizeRows,
      this.conf.maxReadBatchSizeBytes,
      this.conf.maxGpuColumnSizeBytes)
}

case class JsonScanRuleMeta(
    a: JsonScan,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends ScanMeta[JsonScan](a, conf, p, r) {
  override def tagSelfForGpu(): Unit = GpuJsonScan.tagSupport(this)

  override def convertToGpu(): GpuScan =
    GpuJsonScan(a.sparkSession,
      a.fileIndex,
      a.dataSchema,
      a.readDataSchema,
      a.readPartitionSchema,
      a.options,
      a.partitionFilters,
      a.dataFilters,
      this.conf.maxReadBatchSizeRows,
      this.conf.maxReadBatchSizeBytes,
      this.conf.maxGpuColumnSizeBytes)
}

case class HashPartitioningRuleMeta(
    hp: HashPartitioning,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends PartMeta[HashPartitioning](hp, conf, p, r) {
  override val childExprs: Seq[BaseExprMeta[_]] =
    hp.expressions.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))

  private lazy val hashMode = GpuHashPartitioningBase.hashModeFromCpu(hp, this.conf)

  override def tagPartForGpu(): Unit = {
    this.hashMode match {
      case HiveMode =>
        val hh = HiveHash(hp.expressions)
        val hfMeta = GpuOverrides.wrapExpr(hh, this.conf, None)
        hfMeta.tagForGpu()
        if (!hfMeta.canThisBeReplaced) {
          willNotWorkOnGpu(s"the hash function: ${hh.getClass.getSimpleName}" +
            s" can not run on GPU. Details: ${hfMeta.explain(all = false)}")
        }
      case Murmur3Mode =>
        val arrayWithStructsHashing = hp.expressions.exists(e =>
          TrampolineUtil.dataTypeExistsRecursively(e.dataType,
            {
              case ArrayType(_: StructType, _) => true
              case _ => false
            })
        )
        if (arrayWithStructsHashing) {
          willNotWorkOnGpu("hashing arrays with structs is not supported")
        }
      case _ =>
        willNotWorkOnGpu(s"Hash function $hashMode is not supported on GPU")
    }
  }

  override def convertToGpu(): GpuPartitioning =
    GpuHashPartitioning(childExprs.map(_.convertToGpu()), hp.numPartitions,
      this.hashMode)
}

case class RangePartitioningRuleMeta(
    rp: RangePartitioning,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends PartMeta[RangePartitioning](rp, conf, p, r) {
  override val childExprs: Seq[BaseExprMeta[_]] =
    rp.ordering.map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))

  override def convertToGpu(): GpuPartitioning = {
    if (rp.numPartitions > 1) {
      val gpuOrdering = childExprs.map(_.convertToGpu()).asInstanceOf[Seq[SortOrder]]
      GpuRangePartitioning(gpuOrdering, rp.numPartitions)
    } else {
      GpuSinglePartitioning
    }
  }
}

case class RoundRobinPartitioningRuleMeta(
    rrp: RoundRobinPartitioning,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends PartMeta[RoundRobinPartitioning](rrp, conf, p, r) {
  override def convertToGpu(): GpuPartitioning = {
    GpuRoundRobinPartitioning(rrp.numPartitions)
  }
}

case class SinglePartitionRuleMeta(
    sp: SinglePartition.type,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends PartMeta[SinglePartition.type](sp, conf, p, r) {
  override def convertToGpu(): GpuPartitioning = GpuSinglePartitioning
}


case class SaveIntoDataSourceCommandConstructorRuleMeta(
    a: SaveIntoDataSourceCommand,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule)
  extends SaveIntoDataSourceCommandMeta(a, conf, parent, r)
