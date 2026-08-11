/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.lore

import scala.util.control.NonFatal

import com.nvidia.spark.rapids.{DumpUtils, GpuColumnVector, GpuExec, KudoSerializedTableColumn, SerializedTableColumn}
import com.nvidia.spark.rapids.GpuCoalesceExec.EmptyPartition
import com.nvidia.spark.rapids.jni.kudo.KudoSerializer
import com.nvidia.spark.rapids.lore.GpuLore.pathOfChild
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode
import org.apache.hadoop.fs.Path

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.rapids.execution.GpuBroadcastHelper
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration


case class LoreDumpRDDInfo(
    idxInParent: Int,
    loreOutputInfo: LoreOutputInfo,
    attrs: Seq[Attribute],
    hadoopConf: Broadcast[SerializableConfiguration],
    useOriginalSchemaNames: Boolean = false,
    nonStrictMode: Boolean = false)

class GpuLoreDumpRDD(info: LoreDumpRDDInfo, input: RDD[ColumnarBatch])
  extends RDD[ColumnarBatch](input) with GpuLoreRDD with Logging {
  override def rootPath: Path = pathOfChild(info.loreOutputInfo.path, info.idxInParent)
  private val factDataTypes = info.attrs.map(_.dataType)
  lazy val kudoSerializer: KudoSerializer = new KudoSerializer(
    GpuColumnVector.from(factDataTypes.toArray))

  def saveMeta(): Unit = {
    try {
      val meta = LoreRDDMeta(input.getNumPartitions, this.getPartitions.map(_.index), info.attrs)
      GpuLore.dumpObject(meta, pathOfMeta, this.context.hadoopConfiguration)
    } catch {
      case NonFatal(e) if (info.nonStrictMode) =>
        logWarning(s"Failed to save LORE RDD meta to $pathOfMeta: ${e.getMessage}", e)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    if (info.loreOutputInfo.outputLoreId.shouldOutputPartition(split.index)) {
      val originalIter = input.compute(split, context)
      new Iterator[ColumnarBatch] {
        var batchIdx: Int = -1
        var nextBatch: Option[ColumnarBatch] = None

        override def hasNext: Boolean = {
          if (batchIdx == -1) {
            loadNextBatch()
          }
          nextBatch.isDefined
        }

        override def next(): ColumnarBatch = {
          val ret = dumpCurrentBatch()
          loadNextBatch()
          if (!hasNext) {
            // This is the last batch, save the partition meta
            try {
              val isFromShuffle = ret.numCols() == 1 &&
                (ret.column(0).isInstanceOf[SerializedTableColumn] || ret.column(0)
                  .isInstanceOf[KudoSerializedTableColumn])
              val partitionMeta = if (isFromShuffle) {
                // get the array of dataType from the info.attrs
                LoreRDDPartitionMeta(batchIdx, factDataTypes)
              } else {
                LoreRDDPartitionMeta(batchIdx, GpuColumnVector.extractTypes(ret))
              }
              GpuLore.dumpObject(partitionMeta, pathOfPartitionMeta(split.index),
                info.hadoopConf.value.value)
            } catch {
              case NonFatal(e) if (info.nonStrictMode) =>
                logWarning(s"Failed to save LORE partition meta for partition ${split.index}: " +
                  s"${e.getMessage}", e)
            }
          }
          ret
        }

        private def dumpCurrentBatch(): ColumnarBatch = {
          try {
            val outputPath = pathOfBatch(split.index, batchIdx)
            val outputStream = outputPath.getFileSystem(info.hadoopConf.value.value)
              .create(outputPath, true)
            if (info.useOriginalSchemaNames) {
              val colNames = info.attrs.map(_.name)
              val sparkTypes = info.attrs.map(_.dataType)
              DumpUtils.dumpToParquet(nextBatch.get, outputStream, colNames, sparkTypes,
                Some(kudoSerializer))
            } else {
              DumpUtils.dumpToParquet(nextBatch.get, outputStream, Some(kudoSerializer))
            }
          } catch {
            case NonFatal(e) if (info.nonStrictMode) =>
              logWarning(s"Failed to dump LORE batch ${batchIdx} for partition ${split.index}: " +
                s"${e.getMessage}", e)
          }
          nextBatch.get
        }

        private def loadNextBatch(): Unit = {
          if (originalIter.hasNext) {
            nextBatch = Some(originalIter.next())
          } else {
            nextBatch = None
          }
          batchIdx += 1
        }
      }
    } else {
      input.compute(split, context)
    }
  }

  override protected def getPartitions: Array[Partition] = {
    input.partitions
  }
}

class SimpleRDD(_sc: SparkContext, data: Broadcast[Any], schema: StructType) extends
  RDD[ColumnarBatch](_sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    Seq(GpuBroadcastHelper.getBroadcastBatch(data, schema)).iterator
  }

  override protected def getPartitions: Array[Partition] = Array(EmptyPartition(0))
}

/**
 * A reusable wrapper execution node that adds LoRE dumping capability to any SparkPlan
 * that doesn't follow GpuExec's internalDoExecuteColumnar protocol.
 *
 * This can be used by any execution context that needs to wrap a child plan with LoRE
 * dumping functionality, not just data writing commands.
 */
case class GpuLoreDumpExec(
    child: GpuExec,
    loreDumpInfo: LoreDumpRDDInfo)
    extends ShimUnaryExecNode with GpuExec with Logging {

  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${getClass.getCanonicalName} does not support row-based execution")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRDD = child.executeColumnar()
    val rdd = new GpuLoreDumpRDD(loreDumpInfo, childRDD)
    rdd.saveMeta()
    rdd
  }

  override def nodeName: String = s"GpuLoreDumpExec(${child.nodeName})"
}
