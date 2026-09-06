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

package org.apache.spark.sql.delta.rapids.delta42x

import scala.util.Try

import com.nvidia.spark.rapids.delta.DeltaWriteUtils.toBooleanOption

import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.rapids.{GpuDeltaLog, GpuWriteIntoDeltaBase, GpuWriteIntoDeltaLike}

/**
 * GPU version of Delta 4.2's WriteIntoDelta.
 *
 * This class must have a different FQCN from GpuWriteIntoDelta because aggregate JARs contain both
 * the Delta 4.0/4.1 and Delta 4.2 adapters. Sharing an FQCN would cause one version-linked class to
 * replace the other during shading.
 */
case class GpuWriteIntoDelta42x(
    override val gpuDeltaLog: GpuDeltaLog,
    override val cpuWrite: WriteIntoDelta)
  extends GpuWriteIntoDeltaBase(gpuDeltaLog, cpuWrite)
    with GpuWriteIntoDeltaLike {

  override protected def buildCommitMetadata: DeltaOperations.Operation = {
    DeltaOperations.Write(
      cpuWrite.mode,
      Option(cpuWrite.partitionColumns),
      cpuWrite.options.replaceWhere,
      cpuWrite.options.userMetadata,
      toBooleanOption(Try(cpuWrite.options.isDynamicPartitionOverwriteMode).getOrElse(false)),
      toBooleanOption(cpuWrite.options.canOverwriteSchema),
      toBooleanOption(cpuWrite.options.canMergeSchema))
  }

  override protected def copyWithCpuWrite(newCpuWrite: WriteIntoDelta): GpuWriteIntoDelta42x = {
    copy(cpuWrite = newCpuWrite)
  }
}
